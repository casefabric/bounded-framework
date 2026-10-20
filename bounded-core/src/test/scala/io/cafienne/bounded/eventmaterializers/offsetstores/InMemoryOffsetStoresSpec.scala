/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.eventmaterializers.offsetstores

import org.apache.pekko.persistence.query.Offset
import com.typesafe.config.ConfigFactory
import io.cafienne.bounded.eventmaterializers.OffsetStoreProvider
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}

import java.util.concurrent.Executors
import scala.concurrent.{ExecutionContext, Future}

class InMemoryOffsetStoresSpec extends AnyFlatSpec with Matchers with ScalaFutures {

  implicit val patience: PatienceConfig = PatienceConfig(timeout = Span(10, Seconds), interval = Span(50, Millis))

  "InMemoryBasedOffsetStore" should "keep the last offset of every view when many materializers save at once" in {
    val pool                          = Executors.newFixedThreadPool(8)
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(pool)
    val store                         = new InMemoryBasedOffsetStore()
    val views                         = 200
    val savesPerView                  = 50

    try {
      // every view is saved by its own sequence of futures, all views at the same time
      val saves = (1 to views).map { view =>
        (1 to savesPerView).foldLeft(Future.unit) { (previous, n) =>
          previous.flatMap(_ => store.saveOffset(s"view-$view", Offset.sequence(n.toLong)))
        }
      }
      Future.sequence(saves).futureValue

      store.store should have size views.toLong
      (1 to views).foreach { view =>
        store.getOffset(s"view-$view").futureValue shouldBe Offset.sequence(savesPerView.toLong)
      }
    } finally {
      pool.shutdown()
    }
  }

  "OffsetStoreProvider" should "give every materializer the same store, also when they start at the same time" in {
    val pool                          = Executors.newFixedThreadPool(16)
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(pool)
    val config                        = ConfigFactory.parseString("""type = "inmemory"""")

    try {
      val stores = Future.sequence((1 to 64).map(_ => Future(OffsetStoreProvider.getStore(config)))).futureValue

      stores.distinct should have size 1
    } finally {
      pool.shutdown()
    }
  }
}
