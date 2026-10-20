/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.eventmaterializers.offsetstores

import java.util.concurrent.atomic.AtomicInteger

import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.{ExecutionContext, Future}

class CreateOnFirstUseSpec extends AnyFlatSpec with Matchers with ScalaFutures {

  implicit val ec: ExecutionContext = ExecutionContext.global

  behavior of "CreateOnFirstUse"

  it should "create once for all operations" in {
    val creates  = new AtomicInteger()
    val onceOnly = new CreateOnFirstUse(() => Future { creates.incrementAndGet(); () })

    Future.sequence((1 to 50).map(_ => Future(onceOnly()).flatten)).futureValue
    creates.get shouldBe 1
  }

  it should "succeed when a create loses the race with another store and fails once" in {
    val creates   = new AtomicInteger()
    val firstLost = new CreateOnFirstUse(() => {
      if (creates.incrementAndGet() == 1) Future.failed(new RuntimeException("duplicate key value violates unique constraint"))
      else Future.unit
    })

    firstLost().futureValue
    creates.get shouldBe 2
  }

  it should "try again on the next operation when the create keeps failing" in {
    val creates = new AtomicInteger()
    val failing = new CreateOnFirstUse(() => {
      if (creates.incrementAndGet() <= 2) Future.failed(new RuntimeException("database down")) else Future.unit
    })

    failing().failed.futureValue.getMessage shouldBe "database down"
    failing().futureValue
    creates.get shouldBe 3
  }
}
