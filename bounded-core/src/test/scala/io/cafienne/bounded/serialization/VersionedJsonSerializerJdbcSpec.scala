/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.serialization

import org.apache.pekko.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.apache.pekko.persistence.jdbc.testkit.scaladsl.SchemaUtils
import org.apache.pekko.persistence.jdbc.query.scaladsl.JdbcReadJournal
import org.apache.pekko.persistence.query.PersistenceQuery
import org.apache.pekko.stream.scaladsl.Sink
import com.typesafe.config.ConfigFactory
import org.scalatest.wordspec.AnyWordSpecLike
import spray.json._

import java.sql.DriverManager
import scala.util.Using

object VersionedJsonSerializerJdbcSpec {
  val database = "jdbc:h2:mem:versioned-json-serializer;DB_CLOSE_DELAY=-1"

  val config = ConfigFactory
    .parseString(s"""
      pekko.loglevel = INFO
      pekko.actor {
        serializers.versioned = "io.cafienne.bounded.serialization.VersionedJsonSerializerSpec$$TestSerializer"
        serialization-bindings {
          "io.cafienne.bounded.serialization.VersionedJsonSerializerSpec$$PriceSet" = versioned
        }
      }
      pekko.persistence.journal.plugin = "jdbc-journal"
      pekko.persistence.snapshot-store.plugin = "jdbc-snapshot-store"
      pekko-persistence-jdbc.shared-databases.slick {
        profile = "slick.jdbc.H2Profile$$"
        db {
          url = "$database"
          driver = "org.h2.Driver"
          numThreads = 4
          maxConnections = 4
          minConnections = 1
        }
      }
      jdbc-journal.use-shared-db = "slick"
      jdbc-snapshot-store.use-shared-db = "slick"
      jdbc-read-journal.use-shared-db = "slick"
      jdbc-read-journal.refresh-interval = 100ms
    """)
    .withFallback(ConfigFactory.load())
}

/**
  * The serializer bound in the JDBC journal on H2: events are stored as JSON with their versioned manifest, an
  * aggregate recovers from them, and an event stored by an older version is read through the upcasters.
  */
class VersionedJsonSerializerJdbcSpec
    extends ScalaTestWithActorTestKit(VersionedJsonSerializerJdbcSpec.config)
    with AnyWordSpecLike {
  import VersionedJsonSerializerJdbcSpec._
  import VersionedJsonSerializerSpec._

  SchemaUtils.createIfNotExists()(system).futureValue

  private def stored(persistenceId: String): List[(String, String)] =
    Using.resource(DriverManager.getConnection(database)) { connection =>
      val statement = connection.prepareStatement(
        "SELECT \"event_ser_manifest\", \"event_payload\" FROM \"event_journal\" WHERE \"persistence_id\" = ? ORDER BY \"sequence_number\""
      )
      statement.setString(1, persistenceId)
      val result = statement.executeQuery()
      Iterator
        .continually(result)
        .takeWhile(_.next())
        .map(row => row.getString(1) -> new String(row.getBytes(2), "UTF-8"))
        .toList
    }

  "A versioned JSON serializer in the JDBC journal" should {
    "store events as JSON with their manifest and recover the aggregate from them" in {
      val probe = createTestProbe[Long]()
      val first = spawn(prices("jdbc-prices-1"))
      first ! SetPrice(100, probe.ref)
      first ! SetPrice(250, probe.ref)
      probe.receiveMessages(2)
      testKit.stop(first)

      stored("jdbc-prices-1").map { case (manifest, json) => manifest -> json.parseJson } shouldBe List(
        "PriceSet.v3" -> """{"id":"jdbc-prices-1","cents":100,"currency":"EUR"}""".parseJson,
        "PriceSet.v3" -> """{"id":"jdbc-prices-1","cents":250,"currency":"EUR"}""".parseJson
      )

      val recovered = spawn(prices("jdbc-prices-1"))
      recovered ! SetPrice(1, probe.ref)
      probe.expectMessage(351L)
    }

    "read an event stored by an older version through the upcasters" in {
      // As an older release stored it: version 1, a price in euros
      val probe = createTestProbe[Long]()
      val first = spawn(prices("jdbc-prices-2"))
      first ! SetPrice(1, probe.ref)
      probe.receiveMessage()
      testKit.stop(first)
      Using.resource(DriverManager.getConnection(database)) { connection =>
        val update = connection.prepareStatement(
          "UPDATE \"event_journal\" SET \"event_ser_manifest\" = 'PriceSet.v1', \"event_payload\" = ? WHERE \"persistence_id\" = ?"
        )
        update.setBytes(1, """{"id":"jdbc-prices-2","price":12.5}""".getBytes("UTF-8"))
        update.setString(2, "jdbc-prices-2")
        update.executeUpdate(): Unit
      }

      PersistenceQuery(system)
        .readJournalFor[JdbcReadJournal](JdbcReadJournal.Identifier)
        .currentEventsByPersistenceId("jdbc-prices-2", 0, Long.MaxValue)
        .map(_.event)
        .runWith(Sink.seq)
        .futureValue shouldBe List(PriceSet("jdbc-prices-2", 1250, "EUR"))

      val recovered = spawn(prices("jdbc-prices-2"))
      recovered ! SetPrice(0, probe.ref)
      probe.expectMessage(1250L)
    }
  }
}
