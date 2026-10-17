/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.akka.persistence

import org.apache.pekko.persistence.cassandra.query.scaladsl.CassandraReadJournal
import org.apache.pekko.persistence.query.PersistenceQuery
import org.apache.pekko.persistence.query.journal.leveldb.scaladsl.LeveldbReadJournal
import org.apache.pekko.persistence.query.scaladsl._
import io.cafienne.bounded.akka.ActorSystemProvider
import org.apache.pekko.persistence.jdbc.query.scaladsl.JdbcReadJournal
import org.apache.pekko.persistence.r2dbc.query.scaladsl.R2dbcReadJournal
import io.github.alstanchev.pekko.persistence.inmemory.query.scaladsl.InMemoryReadJournal

/**
  * Provides a readJournal that has the eventsByTag available that's used for
  * creation of the domain/query models of the system.
  */
trait ReadJournalProvider { systemProvider: ActorSystemProvider =>
  val configuredJournal =
    system.settings.config.getString("pekko.persistence.journal.plugin")

  def readJournal
    : ReadJournal with CurrentEventsByTagQuery with EventsByTagQuery with CurrentEventsByPersistenceIdQuery = {
    system.log.debug("found configured journal " + configuredJournal)
    if (configuredJournal.endsWith("leveldb")) {
      system.log.debug("configuring read journal for leveldb")
      return PersistenceQuery(system)
        .readJournalFor[LeveldbReadJournal](LeveldbReadJournal.Identifier)
    }
    if (configuredJournal.endsWith("cassandra-journal") || configuredJournal.endsWith("cassandra.journal")) {
      system.log.debug("configuring read journal for cassandra")
      return PersistenceQuery(system)
        .readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)
    }
    if (configuredJournal.endsWith("r2dbc-journal")) {
      return PersistenceQuery(system)
        .readJournalFor[R2dbcReadJournal](R2dbcReadJournal.Identifier)
        .asInstanceOf[
          ReadJournal with CurrentPersistenceIdsQuery with CurrentEventsByPersistenceIdQuery with CurrentEventsByTagQuery with EventsByPersistenceIdQuery with EventsByTagQuery
        ]
    }
    if (configuredJournal.endsWith("jdbc-journal")) {
      return PersistenceQuery(system)
        .readJournalFor[JdbcReadJournal](JdbcReadJournal.Identifier)
//        .asInstanceOf[
//        ReadJournal with CurrentPersistenceIdsQuery with CurrentEventsByPersistenceIdQuery with CurrentEventsByTagQuery with EventsByPersistenceIdQuery with EventsByTagQuery
//      ]
    }
    if (configuredJournal.endsWith("inmemory-journal")) {
      return PersistenceQuery(system)
        .readJournalFor[InMemoryReadJournal](InMemoryReadJournal.Identifier)
        .asInstanceOf[
          ReadJournal with CurrentPersistenceIdsQuery with CurrentEventsByPersistenceIdQuery with CurrentEventsByTagQuery with EventsByPersistenceIdQuery with EventsByTagQuery
        ]
    }
    throw new RuntimeException(
      s"Unsupported read journal $configuredJournal, please switch to cassandra for production"
    )
  }
}
