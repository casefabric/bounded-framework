/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.eventmaterializers.offsetstores

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.persistence.query.{NoOffset, Offset}
import org.apache.pekko.testkit.TestKit
import com.typesafe.config.ConfigFactory
import io.cafienne.bounded.eventmaterializers.ReadJournalOffsetStore
import io.cafienne.bounded.pekko.ActorSystemProvider
import io.cafienne.bounded.pekko.persistence.ReadJournalProvider
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.testcontainers.postgresql.PostgreSQLContainer

/**
  * R2dbcOffsetStore against Postgres in a container; canceled when Docker is not available.
  */
class R2dbcOffsetStoreSpec extends AnyFlatSpec with Matchers with ScalaFutures with BeforeAndAfterAll {
  import DockerTests.dockerAvailable
  import OffsetCodecSpec._

  implicit val patience: PatienceConfig = PatienceConfig(timeout = Span(20, Seconds), interval = Span(100, Millis))

  private var postgres: Option[PostgreSQLContainer] = None
  private var actorSystem: Option[ActorSystem]      = None

  // Started by the first test that runs, so nothing starts when Docker is not available.
  private lazy val system: ActorSystem = {
    val container = new PostgreSQLContainer("postgres:16-alpine")
    container.start()
    postgres = Some(container)
    val config = ConfigFactory
      .parseString(s"""
        pekko.persistence.journal.plugin = "pekko.persistence.r2dbc.journal"
        pekko.persistence.r2dbc.connection-factory {
          host = "${container.getHost}"
          port = ${container.getMappedPort(5432)}
          database = "${container.getDatabaseName}"
          user = "${container.getUsername}"
          password = "${container.getPassword}"
        }
      """)
      .withFallback(ConfigFactory.load())
    val created = ActorSystem("R2dbcOffsetStoreSpec", config)
    actorSystem = Some(created)
    created
  }

  private lazy val store = new R2dbcOffsetStore(system, R2dbcOffsetStore.JournalConnectionFactory)

  behavior of "R2dbcOffsetStore"

  it should "create its table on first use and store and read back every offset type" in {
    assume(dockerAvailable, "Docker is not available")
    allOffsetTypes.zipWithIndex.foreach {
      case (offset, i) =>
        store.saveOffset(s"view-$i", offset).futureValue
        store.getOffset(s"view-$i").futureValue shouldBe offset
    }
  }

  it should "overwrite the offset of a view" in {
    assume(dockerAvailable, "Docker is not available")
    store.saveOffset("view-overwrite", Offset.sequence(1L)).futureValue
    store.saveOffset("view-overwrite", Offset.sequence(2L)).futureValue

    store.getOffset("view-overwrite").futureValue shouldBe Offset.sequence(2L)
  }

  it should "clear the offset of one view and of all views" in {
    assume(dockerAvailable, "Docker is not available")
    store.saveOffset("view-clear-1", Offset.sequence(1L)).futureValue
    store.saveOffset("view-clear-2", Offset.sequence(2L)).futureValue

    store.clear("view-clear-1").futureValue
    store.getOffset("view-clear-1").futureValue shouldBe NoOffset
    store.getOffset("view-clear-2").futureValue shouldBe Offset.sequence(2L)

    store.clear().futureValue
    store.getOffset("view-clear-2").futureValue shouldBe NoOffset
  }

  "A materializer on the Pekko R2DBC journal" should "get the R2DBC offset store, and a clear error for reading by tag" in {
    assume(dockerAvailable, "Docker is not available")
    val provider = new ActorSystemProvider with ReadJournalProvider with ReadJournalOffsetStore {
      override implicit def system: ActorSystem = R2dbcOffsetStoreSpec.this.system
    }

    val error = the[UnsupportedOperationException] thrownBy provider.readJournal
    error.getMessage should include("does not support")
    provider.store shouldBe an[R2dbcOffsetStore]
    provider.saveOffset("view-provider", Offset.sequence(7L)).futureValue
    provider.getOffset("view-provider").futureValue shouldBe Offset.sequence(7L)
  }

  override protected def afterAll(): Unit = {
    actorSystem.foreach(TestKit.shutdownActorSystem(_))
    postgres.foreach(_.stop())
  }
}
