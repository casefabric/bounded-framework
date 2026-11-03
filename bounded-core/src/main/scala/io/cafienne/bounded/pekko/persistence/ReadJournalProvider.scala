/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.pekko.persistence

import org.apache.pekko.persistence.cassandra.query.scaladsl.CassandraReadJournal
import org.apache.pekko.persistence.query.PersistenceQuery
import org.apache.pekko.persistence.query.journal.leveldb.scaladsl.LeveldbReadJournal
import org.apache.pekko.persistence.query.scaladsl._
import io.cafienne.bounded.pekko.ActorSystemProvider
import scala.annotation.nowarn
import org.apache.pekko.persistence.jdbc.query.scaladsl.JdbcReadJournal
import io.github.alstanchev.pekko.persistence.inmemory.query.scaladsl.InMemoryReadJournal
import org.apache.pekko.persistence.r2dbc.query.scaladsl.R2dbcReadJournal

/**
  * Provides the read journal of the configured journal plugin, which the event materializers read the events of
  * an aggregate type from.
  */
trait ReadJournalProvider { systemProvider: ActorSystemProvider =>
  val configuredJournal =
    system.settings.config.getString("pekko.persistence.journal.plugin")

  /**
    * Whether the configured journal is the R2DBC journal, which has no queries by tag.
    */
  def isR2dbcJournal: Boolean = ReadJournalProvider.isR2dbcJournal(configuredJournal)

  /**
    * The events per aggregate type: by tag on the tag-based journals, by entity type on the R2DBC journal.
    */
  def aggregateTypeJournal: AggregateTypeReadJournal =
    if (isR2dbcJournal) new R2dbcAggregateTypeReadJournal(system)
    else new TagAggregateTypeReadJournal(readJournal)

  /**
    * The read journal with the queries by tag, on every journal but R2DBC.
    */
  @nowarn("cat=deprecation") // LevelDB is deprecated in Pekko but still supported here
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
    // queries by tag; aggregateTypeJournal reads it by entity type.
    if (isR2dbcJournal) {
      throw new UnsupportedOperationException(
        s"The R2DBC journal $configuredJournal has no queries by tag (it only offers eventsBySlices); " +
          "read events by aggregate type with aggregateTypeJournal"
      )
    }
    if (configuredJournal.endsWith("jdbc-journal")) {
      return PersistenceQuery(system)
        .readJournalFor[JdbcReadJournal](JdbcReadJournal.Identifier)
    }
    if (configuredJournal.endsWith("inmemory-journal")) {
      return PersistenceQuery(system)
        .readJournalFor[InMemoryReadJournal](InMemoryReadJournal.Identifier)
        .asInstanceOf[
          ReadJournal
            with CurrentPersistenceIdsQuery
            with CurrentEventsByPersistenceIdQuery
            with CurrentEventsByTagQuery
            with EventsByPersistenceIdQuery
            with EventsByTagQuery
        ]
    }
    throw new RuntimeException(
      s"Unsupported read journal $configuredJournal, please switch to cassandra for production"
    )
  }
}

object ReadJournalProvider {

  /**
    * The plugin id of the read journal that belongs to the configured journal plugin.
    */
  def queryPluginId(configuredJournal: String): String =
    if (configuredJournal.endsWith("leveldb")) LeveldbReadJournal.Identifier
    else if (configuredJournal.endsWith("cassandra-journal") || configuredJournal.endsWith("cassandra.journal"))
      CassandraReadJournal.Identifier
    else if (isR2dbcJournal(configuredJournal)) R2dbcReadJournal.Identifier
    else if (configuredJournal.endsWith("jdbc-journal")) JdbcReadJournal.Identifier
    else if (configuredJournal.endsWith("inmemory-journal")) InMemoryReadJournal.Identifier
    else throw new IllegalArgumentException(s"Unsupported journal $configuredJournal")

  // The Pekko plugin is pekko.persistence.r2dbc.journal; r2dbc-journal is accepted for a plugin configured under
  // that name.
  def isR2dbcJournal(configuredJournal: String): Boolean =
    configuredJournal.endsWith("r2dbc.journal") || configuredJournal.endsWith("r2dbc-journal")
}
