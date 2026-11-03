/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.pekko.persistence

import org.apache.pekko.NotUsed
import org.apache.pekko.persistence.query.{EventEnvelope, Offset}
import org.apache.pekko.persistence.query.scaladsl.{CurrentEventsByTagQuery, EventsByTagQuery}
import org.apache.pekko.stream.scaladsl.Source
import io.cafienne.bounded.aggregate.AggregateType

/**
  * The events of one aggregate type, as the event materializers read them. Every journal stores the type in its own
  * way: as a tag on the tag-based journals (Cassandra, JDBC, LevelDB, in-memory), and as the entity type of the
  * persistence id on the R2DBC journal. `ReadJournalProvider.aggregateTypeJournal` gives the one for the configured
  * journal.
  *
  * The offsets are those of the journal and are only meaningful on the journal that produced them.
  */
trait AggregateTypeReadJournal {

  /**
    * The stored events of the aggregate type after the offset; the stream completes at the end of them.
    */
  def currentEventsByAggregateType(aggregateType: AggregateType, offset: Offset): Source[EventEnvelope, NotUsed]

  /**
    * The events of the aggregate type after the offset, including those stored later; the stream does not complete.
    */
  def eventsByAggregateType(aggregateType: AggregateType, offset: Offset): Source[EventEnvelope, NotUsed]
}

/**
  * Reads an aggregate type by its tag: every aggregate tags each of its events with its type.
  */
final class TagAggregateTypeReadJournal(journal: CurrentEventsByTagQuery with EventsByTagQuery)
    extends AggregateTypeReadJournal {

  override def currentEventsByAggregateType(
    aggregateType: AggregateType,
    offset: Offset
  ): Source[EventEnvelope, NotUsed] =
    journal.currentEventsByTag(aggregateType.name, offset)

  override def eventsByAggregateType(aggregateType: AggregateType, offset: Offset): Source[EventEnvelope, NotUsed] =
    journal.eventsByTag(aggregateType.name, offset)
}
