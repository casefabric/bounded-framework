/*
 * Copyright (C) 2016-2018 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.test

import akka.persistence.inmemory.query.scaladsl.InMemoryReadJournal
import akka.persistence.query.PersistenceQuery
import akka.persistence.query.scaladsl._
import akka.testkit.TestKit
import org.scalatest.Suite

trait InMemoryReadJournalProvider {
  this: TestKit with Suite =>
  def eventsQuery =
    PersistenceQuery(system)
      .readJournalFor[InMemoryReadJournal](InMemoryReadJournal.Identifier)
      .asInstanceOf[ReadJournal with CurrentPersistenceIdsQuery with CurrentEventsByPersistenceIdQuery with CurrentEventsByTagQuery with EventsByPersistenceIdQuery with EventsByTagQuery]
}
