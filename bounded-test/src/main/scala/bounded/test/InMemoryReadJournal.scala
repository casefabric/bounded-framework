package bounded.test

import akka.persistence.query.PersistenceQuery
import akka.persistence.query.scaladsl._
import akka.testkit.TestKit
import org.scalatest.Suite

trait InMemoryReadJournal {
  this: TestKit with Suite =>
  def eventsQuery =
    PersistenceQuery(system)
      .readJournalFor("inmemory-read-journal")
      .asInstanceOf[ReadJournal with CurrentPersistenceIdsQuery with CurrentEventsByPersistenceIdQuery with CurrentEventsByTagQuery with EventsByPersistenceIdQuery with EventsByTagQuery]
}
