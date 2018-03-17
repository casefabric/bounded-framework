/*
 * Copyright (C) 2016-2018 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.akka.persistence

import akka.actor.Props
import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.inmemory.query.scaladsl.InMemoryReadJournal
import akka.persistence.journal.leveldb.{
  SharedLeveldbJournal,
  SharedLeveldbStore
}
import akka.persistence.query.PersistenceQuery
import akka.persistence.query.journal.leveldb.scaladsl.LeveldbReadJournal
import akka.persistence.query.scaladsl._
import io.cafienne.bounded.akka.ActorSystemProvider
import io.cafienne.bounded.akka.persistence.leveldb.SharedJournal

/**
  * Provides a readJournal that has the eventsByTag available that's used for
  * creation of the domain/query models of the system.
  */
trait ReadJournalProvider { systemProvider: ActorSystemProvider =>

  val configuredJournal =
    system.settings.config.getString("akka.persistence.journal.plugin")

  def readJournal
    : ReadJournal with CurrentEventsByTagQuery with EventsByTagQuery with CurrentEventsByPersistenceIdQuery = {
    system.log.debug("found configured journal " + configuredJournal)
    if (configuredJournal.endsWith("leveldb")) {
      system.log.debug("configuring read journal for leveldb")
      return PersistenceQuery(system)
        .readJournalFor[LeveldbReadJournal](LeveldbReadJournal.Identifier)
    }
    if (configuredJournal.endsWith("leveldb-shared")) {
      system.log.debug("configuring read journal for leveldb-shared")

      val sharedJournal =
        system.actorOf(Props(new SharedLeveldbStore), SharedJournal.name)
      SharedLeveldbJournal.setStore(sharedJournal, system)

      return PersistenceQuery(system)
        .readJournalFor[LeveldbReadJournal](LeveldbReadJournal.Identifier)
    }
    if (configuredJournal.endsWith("cassandra-journal")) {
      system.log.debug("configuring read journal for cassandra")
      return PersistenceQuery(system)
        .readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)
    }
    if (configuredJournal.endsWith("inmemory-journal")) {
      return PersistenceQuery(system)
        .readJournalFor[InMemoryReadJournal](InMemoryReadJournal.Identifier)
        .asInstanceOf[ReadJournal with CurrentPersistenceIdsQuery with CurrentEventsByPersistenceIdQuery with CurrentEventsByTagQuery with EventsByPersistenceIdQuery with EventsByTagQuery]
    }
    throw new RuntimeException(
      s"Unsupported read journal $configuredJournal, please switch to cassandra for production")
  }
}
