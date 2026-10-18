/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.eventmaterializers.offsetstores

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.persistence.cassandra.query.scaladsl.CassandraReadJournal
import org.apache.pekko.persistence.query.{NoOffset, Offset, PersistenceQuery}
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
import org.testcontainers.cassandra.CassandraContainer

/**
  * CassandraOffsetStore against Cassandra in a container, set up through ReadJournalOffsetStore as an event
  * materializer gets it; canceled when Docker is not available.
  */
class CassandraOffsetStoreIntegrationSpec extends AnyFlatSpec with Matchers with ScalaFutures with BeforeAndAfterAll {
  import DockerTests.dockerAvailable
  import OffsetCodecSpec._

  implicit val patience: PatienceConfig = PatienceConfig(timeout = Span(30, Seconds), interval = Span(100, Millis))

  private var cassandra: Option[CassandraContainer] = None
  private var actorSystem: Option[ActorSystem]      = None

  // Started by the first test that runs, so nothing starts when Docker is not available.
  private lazy val provider = {
    val container = new CassandraContainer("cassandra:4.1")
    container.start()
    cassandra = Some(container)
    val contactPoint = container.getContactPoint
    val config = ConfigFactory
      .parseString(s"""
        pekko.persistence.journal.plugin = "pekko.persistence.cassandra.journal"
        datastax-java-driver {
          basic.contact-points = ["${contactPoint.getHostString}:${contactPoint.getPort}"]
          basic.load-balancing-policy.local-datacenter = "${container.getLocalDatacenter}"
        }
      """)
      .withFallback(ConfigFactory.load())
    val created = ActorSystem("CassandraOffsetStoreIntegrationSpec", config)
    actorSystem = Some(created)
    // The journal creates its keyspace when it starts; here only the offset store is used.
    PersistenceQuery(created)
      .readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)
      .session
      .executeWrite(
        "CREATE KEYSPACE IF NOT EXISTS pekko WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
      )
      .futureValue
    new ActorSystemProvider with ReadJournalProvider with ReadJournalOffsetStore {
      override implicit def system: ActorSystem = created
    }
  }

  behavior of "CassandraOffsetStore"

  it should "be set up in the keyspace of the Pekko Cassandra journal" in {
    assume(dockerAvailable, "Docker is not available")
    provider.store shouldBe a[CassandraOffsetStore]
    CassandraOffsetStore.keyspace(provider.system.settings.config, provider.configuredJournal) shouldBe "pekko"
  }

  it should "store and read back every offset type" in {
    assume(dockerAvailable, "Docker is not available")
    allOffsetTypes.zipWithIndex.foreach {
      case (offset, i) =>
        provider.saveOffset(s"view-$i", offset).futureValue
        provider.getOffset(s"view-$i").futureValue shouldBe offset
    }
  }

  it should "overwrite the offset of a view and read NoOffset for an unknown view" in {
    assume(dockerAvailable, "Docker is not available")
    provider.saveOffset("view-overwrite", Offset.sequence(1L)).futureValue
    provider.saveOffset("view-overwrite", Offset.sequence(2L)).futureValue

    provider.getOffset("view-overwrite").futureValue shouldBe Offset.sequence(2L)
    provider.getOffset("view-unknown").futureValue shouldBe NoOffset
  }

  override protected def afterAll(): Unit = {
    actorSystem.foreach(TestKit.shutdownActorSystem(_))
    cassandra.foreach(_.stop())
  }
}
