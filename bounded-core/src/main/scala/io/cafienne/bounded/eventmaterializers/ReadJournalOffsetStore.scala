/*
 * Copyright (C) 2016-2018 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.eventmaterializers

import java.util.concurrent.TimeUnit

import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.query.Offset
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import io.cafienne.bounded.akka.ActorSystemProvider
import io.cafienne.bounded.akka.persistence.ReadJournalProvider
import io.cafienne.bounded.eventmaterializers.offsetstores._

import scala.concurrent.Future
import scala.concurrent.duration._

trait ReadJournalOffsetStore extends OffsetStore {
  this: ReadJournalProvider with ActorSystemProvider =>

  val store: OffsetStore = {

    if (configuredJournal.endsWith("cassandra-journal")) {
      val keyspace = system.settings.config.getString("cassandra-journal.keyspace")
      new CassandraOffsetStore(
        readJournal.asInstanceOf[CassandraReadJournal],
        cassandraCreateTableTimeout,
        keyspace = keyspace
      )
    } else if (configuredJournal.endsWith("inmemory-journal")) {
      new InMemoryBasedOffsetStore()
    } else if (configuredJournal.endsWith("leveldb")) {
      val levelDbDir = system.settings.config.getString("akka.persistence.journal.leveldb.dir")
      val c          = ConfigFactory.empty().withValue("path", ConfigValueFactory.fromAnyRef(levelDbDir + "/lmdb_offsets"))
      LmdbOffsetStore(new LmdbConfig(c))
    } else {
      throw new RuntimeException(s"Offsetstore $configuredJournal is not supported as ReadJournalOffsetStore")
    }
  }

  private def cassandraCreateTableTimeout = {
    val timeout =
      system.settings.config.getDuration("bounded.eventmaterializers.cassandra-offsetstore.createtable-timeout")
    FiniteDuration(timeout.toNanos, TimeUnit.NANOSECONDS)
  }

  override def saveOffset(viewIdentifier: String, offset: Offset): Future[Unit] =
    store.saveOffset(viewIdentifier, offset)

  override def getOffset(viewIdentifier: String): Future[Offset] =
    store.getOffset(viewIdentifier)

}
