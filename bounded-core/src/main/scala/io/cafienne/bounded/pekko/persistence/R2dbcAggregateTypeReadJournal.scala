/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.pekko.persistence

import org.apache.pekko.NotUsed
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.event.Logging
import org.apache.pekko.persistence.Persistence
import org.apache.pekko.persistence.query.{EventEnvelope, NoOffset, Offset, PersistenceQuery, TimestampOffset}
import org.apache.pekko.persistence.query.typed.{EventEnvelope => SliceEnvelope}
import org.apache.pekko.persistence.r2dbc.query.scaladsl.R2dbcReadJournal
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import io.cafienne.bounded.aggregate.AggregateType

import scala.concurrent.{ExecutionContext, Future}

/**
  * Reads an aggregate type from the R2DBC journal by its entity type: the journal stores the part of the persistence
  * id before `|` (`aggregateType|aggregateId`) in its own column, and queries it per range of slices. The R2DBC journal
  * has no queries by tag.
  *
  * The plugin leaves part of the work to the reader, which is done here:
  *  - its backtracking queries, which re-read a recent window to catch events committed late, deliver events again:
  *    an event at or below the last sequence number delivered for its persistence id is dropped;
  *  - an event found by backtracking comes without its payload, which is then loaded;
  *  - should an event arrive with a sequence number more than one past the last one delivered for its persistence id,
  *    the events in between are read first, so the events of one aggregate always arrive in order.
  *
  * The last delivered sequence numbers are kept for at most `bounded.eventmaterializers.r2dbc.deduplicate-capacity`
  * persistence ids (the least recently seen are dropped), seeded from the `seen` map of the start offset. For a
  * persistence id that was dropped an event can arrive twice. Delivery is at-least-once, as on every journal.
  *
  * After a restart from a stored offset the plugin's first backtracking query starts
  * `pekko.persistence.r2dbc.query.backtracking.window` plus its `behind-current-time` before that offset (2 minutes
  * and 10 seconds by default), so the events stored in that time before the offset are handled again, except those
  * at the offset itself. Only one offset per materializer is stored, so this is what keeps an event committed late
  * just before a stop from being lost. A smaller window means fewer repeats, but it must still cover the longest
  * time between the start and the commit of a transaction that writes events.
  *
  * Offsets are `TimestampOffset`s. Any other offset was stored on another journal and means nothing here: the events
  * are then read from the start, once.
  */
final class R2dbcAggregateTypeReadJournal(system: ActorSystem) extends AggregateTypeReadJournal {
  import R2dbcAggregateTypeReadJournal._

  private val log                           = Logging(system, classOf[R2dbcAggregateTypeReadJournal])
  private implicit val ec: ExecutionContext = system.dispatcher

  private val journal: R2dbcReadJournal =
    PersistenceQuery(system).readJournalFor[R2dbcReadJournal](R2dbcReadJournal.Identifier)

  private val maxSlice = Persistence(system).numberOfSlices - 1

  private val capacity = system.settings.config.getInt(DeduplicateCapacityPath)

  override def currentEventsByAggregateType(
    aggregateType: AggregateType,
    offset: Offset
  ): Source[EventEnvelope, NotUsed] =
    inOrderOnce(offset)(start => journal.currentEventsBySlices[Any](aggregateType.name, 0, maxSlice, start))

  override def eventsByAggregateType(aggregateType: AggregateType, offset: Offset): Source[EventEnvelope, NotUsed] =
    inOrderOnce(offset)(start => journal.eventsBySlices[Any](aggregateType.name, 0, maxSlice, start))

  private def inOrderOnce(
    offset: Offset
  )(query: Offset => Source[SliceEnvelope[Any], NotUsed]): Source[EventEnvelope, NotUsed] = {
    val start = startOffset(offset)
    Source
      .fromMaterializer { (materializer, _) =>
        val delivered = new DeliveredSequenceNrs(capacity)
        start match {
          case timestampOffset: TimestampOffset =>
            timestampOffset.seen.foreach { case (pid, seqNr) =>
              delivered.update(pid, seqNr)
            }
          case _ => ()
        }
        query(start).mapAsync(1)(deliver(delivered, _)(materializer)).mapConcat(identity)
      }
      .mapMaterializedValue(_ => NotUsed)
  }

  private def startOffset(offset: Offset): Offset = offset match {
    case NoOffset | _: TimestampOffset => offset
    case other                         =>
      log.warning(
        "Offset {} is not an offset of the R2DBC journal (it was stored on another journal): reading from the start",
        other
      )
      NoOffset
  }

  // Runs one envelope at a time (mapAsync(1)), so `delivered` is never used concurrently.
  private def deliver(delivered: DeliveredSequenceNrs, envelope: SliceEnvelope[Any])(
    implicit materializer: Materializer
  ): Future[List[EventEnvelope]] = {
    val persistenceId = envelope.persistenceId
    val sequenceNr    = envelope.sequenceNr
    delivered.get(persistenceId) match {
      case Some(last) if sequenceNr <= last =>
        Future.successful(Nil)
      case Some(last) if sequenceNr > last + 1 =>
        log.debug("Reading events {} to {} of {} first", last + 1, sequenceNr - 1, persistenceId)
        for {
          missing <- journal.currentEventsByPersistenceId(persistenceId, last + 1, sequenceNr - 1).runWith(Sink.seq)
          current <- withPayload(envelope)
        } yield {
          delivered.update(persistenceId, sequenceNr)
          missing.toList :+ current
        }
      case _ =>
        withPayload(envelope).map { current =>
          delivered.update(persistenceId, sequenceNr)
          List(current)
        }
    }
  }

  private def withPayload(envelope: SliceEnvelope[Any]): Future[EventEnvelope] =
    envelope.eventOption match {
      case Some(event) => Future.successful(classic(envelope, event))
      case None        =>
        journal
          .loadEnvelope[Any](envelope.persistenceId, envelope.sequenceNr)
          .map(loaded => classic(envelope, loaded.event))
    }

  private def classic(envelope: SliceEnvelope[Any], event: Any): EventEnvelope =
    EventEnvelope(envelope.offset, envelope.persistenceId, envelope.sequenceNr, event, envelope.timestamp)
}

object R2dbcAggregateTypeReadJournal {

  val DeduplicateCapacityPath = "bounded.eventmaterializers.r2dbc.deduplicate-capacity"

  /**
    * The last delivered sequence number per persistence id, for at most `maxPersistenceIds` persistence ids: the
    * least recently used ones are dropped.
    */
  private[persistence] final class DeliveredSequenceNrs(maxPersistenceIds: Int) {
    private val sequenceNrs = new LeastRecentlyUsed(maxPersistenceIds)

    def get(persistenceId: String): Option[Long] = Option(sequenceNrs.get(persistenceId)).map(_.longValue)

    def update(persistenceId: String, sequenceNr: Long): Unit =
      if (get(persistenceId).forall(_ < sequenceNr)) {
        sequenceNrs.put(persistenceId, sequenceNr): Unit
      }

    def persistenceIds: Int = sequenceNrs.size()
  }

  private final class LeastRecentlyUsed(maxEntries: Int)
      extends java.util.LinkedHashMap[String, java.lang.Long](16, 0.75f, true) {
    override def removeEldestEntry(eldest: java.util.Map.Entry[String, java.lang.Long]): Boolean =
      size() > maxEntries
  }
}
