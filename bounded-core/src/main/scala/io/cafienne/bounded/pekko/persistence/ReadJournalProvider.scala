/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.pekko.persistence

import org.apache.pekko.persistence.cassandra.query.scaladsl.CassandraReadJournal
import org.apache.pekko.persistence.query.PersistenceQuery
import org.apache.pekko.persistence.query.journal.leveldb.scaladsl.LeveldbReadJournal
import org.apache.pekko.persistence.query.scaladsl._
import io.cafienne.bounded.pekko.ActorSystemProvider
import org.apache.pekko.persistence.jdbc.query.scaladsl.JdbcReadJournal
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
    // The Pekko plugin is pekko.persistence.r2dbc.journal. Its read journal offers eventsBySlices but no
    // queries by tag, which the event materializers are built on.
    if (configuredJournal.endsWith("r2dbc.journal") || configuredJournal.endsWith("r2dbc-journal")) {
      throw new UnsupportedOperationException(
        s"Event materializers read events by tag, which the R2DBC journal $configuredJournal does not support " +
        "(it only offers eventsBySlices)"
      )
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
