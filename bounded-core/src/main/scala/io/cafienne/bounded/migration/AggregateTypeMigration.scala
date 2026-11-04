/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.migration

import org.apache.pekko.Done
import org.apache.pekko.actor.{ActorRef, ActorSystem, PoisonPill, Props, Status}
import org.apache.pekko.event.Logging
import org.apache.pekko.pattern.{after, ask}
import org.apache.pekko.persistence.{
  DeleteMessagesFailure,
  DeleteMessagesSuccess,
  DeleteSnapshotsFailure,
  DeleteSnapshotsSuccess,
  PersistentActor,
  Recovery,
  RecoveryCompleted,
  SaveSnapshotFailure,
  SaveSnapshotSuccess,
  SnapshotOffer,
  SnapshotSelectionCriteria
}
import org.apache.pekko.persistence.journal.Tagged
import org.apache.pekko.persistence.query.{NoOffset, Offset, PersistenceQuery, TimestampOffset}
import org.apache.pekko.persistence.query.scaladsl.{
  CurrentEventsByPersistenceIdQuery,
  CurrentEventsByTagQuery,
  CurrentPersistenceIdsQuery,
  ReadJournal
}
import org.apache.pekko.serialization.SerializationExtension
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.apache.pekko.util.Timeout
import io.cafienne.bounded.aggregate.AggregateType
import io.cafienne.bounded.eventmaterializers.TagOffsets
import io.cafienne.bounded.eventmaterializers.offsetstores.OffsetStore
import io.cafienne.bounded.pekko.ActorSystemProvider
import io.cafienne.bounded.pekko.persistence.{AggregateTypeReadJournal, ReadJournalProvider}

import scala.collection.immutable.ListMap
import java.time.Instant
import java.util.concurrent.TimeoutException
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}

/**
  * Where an event of an old stream goes: the stream `aggregateType|aggregateId`, with the extra tags it gets besides
  * the aggregate type (as `AggregateRootActor.tags` gives them). Extra tags of the old events are not copied by
  * themselves: the journal queries do not return them.
  */
final case class MigrationTarget(aggregateType: AggregateType, aggregateId: String, tags: Set[String] = Set.empty) {
  def persistenceId: String = aggregateType.persistenceId(aggregateId).id
}

/**
  * What happens, or happened, to one old stream.
  *
  * @param persistenceId the old persistence id
  * @param events the number of events in the old stream
  * @param targets the new streams with the number of events each gets, in the order they first appear
  * @param unmapped the number of events the rule gives no target for; a stream with any is left alone
  * @param snapshotSequenceNr the sequence number of the latest snapshot of the old stream
  * @param untagged for a stream that is already in place, the number of its events that are not tagged with its type
  *                 on a tag-based journal
  */
final case class StreamMigration(
  persistenceId: String,
  events: Long,
  targets: ListMap[String, Long],
  unmapped: Long,
  snapshotSequenceNr: Option[Long],
  untagged: Long = 0L
) {

  /** Whether the rule puts every event of the stream back in the stream itself: it is already `type|id`. */
  def inPlace: Boolean = unmapped == 0 && targets.keySet == Set(persistenceId)

  /** Whether the stream is copied: all its events have a target, it has at least one, and it is not in place. */
  def migrates: Boolean = unmapped == 0 && targets.nonEmpty && !inPlace

  /** Whether the events go to more than one new stream; the snapshot is then dropped. */
  def split: Boolean = targets.size > 1

  /** Whether the latest snapshot is copied: the stream migrates and is not split. */
  def copiesSnapshot: Boolean = migrates && !split && snapshotSequenceNr.isDefined

  override def toString: String =
    if (inPlace && untagged > 0)
      s"$persistenceId: in place, but $untagged of its $events events are not tagged with its aggregate type, so " +
        "event materializers do not see them"
    else if (inPlace) s"$persistenceId: in place ($events events)"
    else if (!migrates) s"$persistenceId: left alone ($events events, $unmapped without target)"
    else {
      val to       = targets.map { case (pid, count) => s"$pid ($count)" }.mkString(", ")
      val snapshot = snapshotSequenceNr.fold("no snapshot")(nr =>
        if (copiesSnapshot) s"snapshot at $nr copied" else s"snapshot at $nr dropped (split)"
      )
      s"$persistenceId: $events events to $to, $snapshot"
    }
}

/**
  * The streams a migration looked at; the conflicts that keep it from copying anything; and for a run that copied, the
  * new streams whose events do not match the plan (expected and found number of events).
  */
final case class MigrationReport(
  streams: Seq[StreamMigration],
  conflicts: Seq[String] = Nil,
  mismatches: Map[String, (Long, Long)] = Map.empty
) {
  def migrating: Seq[StreamMigration] = streams.filter(_.migrates)
  def inPlace: Seq[StreamMigration]   = streams.filter(_.inPlace)
  def leftAlone: Seq[StreamMigration] = streams.filterNot(stream => stream.migrates || stream.inPlace)

  /** Streams already in place whose events materializers on a tag-based journal do not see. */
  def untagged: Seq[StreamMigration] = inPlace.filter(_.untagged > 0)

  /** Whether there are no conflicts and every new stream holds exactly the events planned for it. */
  def verified: Boolean = conflicts.isEmpty && mismatches.isEmpty

  override def toString: String =
    (s"${migrating.size} streams to migrate, ${inPlace.size} in place, ${leftAlone.size} left alone" +:
      (conflicts.map(conflict => s"CONFLICT $conflict") ++ streams.map(_.toString) ++ mismatches.map {
        case (pid, (expected, found)) => s"MISMATCH $pid: expected $expected events, found $found"
      })).mkString("\n")
}

/**
  * Converts a journal written without aggregate types (Bounded 1.0.0 and earlier: persistence ids without a type, or
  * with a prefix other than `type|`; typed aggregates on `PersistenceId.ofUniqueId`) into streams `type|id` whose
  * events are tagged with the type, as Bounded 2 writes them.
  *
  * It runs in the application's own actor system, with the production journal and snapshot store config, and uses
  * only Pekko's persistence and query APIs, so it works on every journal Bounded supports. Events are copied: the
  * journal itself writes the new rows, so their tags, and on R2DBC their entity type and slice, are right. The copies
  * get new write timestamps, and event metadata is not copied. Events are written as they are read; on a tag-based
  * journal, or with extra tags, they are wrapped in `Tagged`, which a write-side event adapter bound to an event class
  * does not see.
  *
  * Streams the rule maps onto themselves are in place and not copied. On a tag-based journal their events must carry
  * the type as a tag; the report lists those that do not (`untagged`), e.g. 1.0.0 streams of a prefix `type|` whose
  * events were not tagged with that name. Copying cannot fix those in place: map them to another type name, or tag
  * them with the journal's own tools.
  *
  * The application must not run while migrating: an event written to an old stream after it was copied is lost.
  * The order:
  *  1. stop the application;
  *  1. `dryRun()` and check the report: which streams go where, which are split, which are left alone, which are in
  *     place but untagged, and that there are no conflicts;
  *  1. `migrate()`, which can be run again after an interruption and then continues where it stopped;
  *  1. check that the returned report is `verified`;
  *  1. `deleteMigrated(report)`: old streams and their snapshots are deleted. Until then old and new events exist side
  *     by side; on a tag-based journal an old event can carry the same tag (e.g. from an event adapter), so no
  *     materializer may replay before this step;
  *  1. `oldStreamsReadableByType(...)` for every aggregate type must be empty. Cassandra keeps deleted events in its
  *     tag views: when old events were tagged with the name of their new type, clean those with the plugin's
  *     reconciliation tool;
  *  1. for every combined logic `startCombinedLogicAtEnd(...)`, so it does not handle the copies (and repeat its side effects); for every
  *     other event materializer `startAtEnd(...)` when it keeps the data it already has, or `resetOffsets(...)` when its
  *     data is cleared and rebuilt: the copies have new offsets, so a materializer left as it is handles every copy;
  *  1. start the new version of the application.
  *
  * @param system the actor system with the journal and snapshot store to migrate
  * @param rule the new stream of an event, given the old persistence id and the event; None for an event that has
  *             none, which leaves its whole stream alone. Events of one old stream with different targets split it.
  *             Two old streams cannot share a target, and a target cannot be another stream that is migrated.
  * @param selectStream which old persistence ids to look at; by default all of them
  * @param batchSize the number of events written at once
  * @param parallelism the number of old streams migrated at the same time
  * @param timeout the time one journal or snapshot store operation may take
  */
class AggregateTypeMigration(
  system: ActorSystem,
  rule: (String, Any) => Option[MigrationTarget],
  selectStream: String => Boolean = AggregateTypeMigration.allStreams,
  batchSize: Int = 100,
  parallelism: Int = 4
)(implicit timeout: Timeout = Timeout(30.seconds)) {
  import AggregateTypeMigration._

  require(batchSize > 0, "batchSize must be positive")
  require(parallelism > 0, "parallelism must be positive")

  private implicit val actorSystem: ActorSystem = system
  private implicit val ec: ExecutionContext     = system.dispatcher
  private val log                               = Logging(system, classOf[AggregateTypeMigration])
  private val config                            = system.settings.config
  private val serialization                     = SerializationExtension(system)

  private val configuredJournal = config.getString("pekko.persistence.journal.plugin")
  private val isR2dbc           = ReadJournalProvider.isR2dbcJournal(configuredJournal)

  // Without a configured snapshot store Pekko uses one that holds nothing and fails every delete.
  private val hasSnapshotStore = {
    val plugin = config.getString("pekko.persistence.snapshot-store.plugin")
    plugin.nonEmpty && plugin != "pekko.persistence.no-snapshot-store"
  }

  private val journal: ReadJournal with CurrentPersistenceIdsQuery with CurrentEventsByPersistenceIdQuery =
    PersistenceQuery(system)
      .readJournalFor[ReadJournal with CurrentPersistenceIdsQuery with CurrentEventsByPersistenceIdQuery](
        ReadJournalProvider.queryPluginId(configuredJournal)
      )

  private lazy val tagJournal: ReadJournal with CurrentEventsByTagQuery =
    PersistenceQuery(system)
      .readJournalFor[ReadJournal with CurrentEventsByTagQuery](ReadJournalProvider.queryPluginId(configuredJournal))

  private lazy val typeJournal: AggregateTypeReadJournal = {
    val migrationSystem = system
    new ActorSystemProvider with ReadJournalProvider {
      implicit def system: ActorSystem = migrationSystem
    }.aggregateTypeJournal
  }

  /**
    * What `migrate()` would do, without writing anything.
    */
  def dryRun(): Future[MigrationReport] = planAll()

  /**
    * Copies every selected stream whose events all have a target, and verifies the new streams. Streams copied by an
    * earlier, interrupted run are completed, not copied again. Copies nothing when the plan has conflicts. Fails when
    * a new stream already holds events that are not the copies of its old stream's events.
    */
  def migrate(): Future[MigrationReport] =
    planAll().flatMap { planned =>
      if (planned.conflicts.nonEmpty) Future.successful(planned)
      else
        Source(planned.streams.toList)
          .mapAsync(parallelism)(stream =>
            if (stream.migrates) copy(stream).map(_ => stream) else Future.successful(stream)
          )
          .runWith(Sink.ignore)
          .flatMap(_ => verify(planned))
    }

  /**
    * Checks that every new stream of the report holds the planned number of events, numbered without gaps.
    */
  def verify(report: MigrationReport): Future[MigrationReport] = {
    val expected = report.migrating.flatMap(_.targets).groupMapReduce(_._1)(_._2)(_ + _)
    Source(expected.toList)
      .mapAsync(parallelism) { case (pid, count) =>
        for {
          highest <- highestSequenceNr(pid)
          stored  <- journal.currentEventsByPersistenceId(pid, 0L, Long.MaxValue).runFold(0L)((n, _) => n + 1)
        } yield pid -> (count, stored, highest)
      }
      .collect { case (pid, (count, stored, highest)) if stored != count || highest != count => pid -> (count, stored) }
      .runWith(Sink.seq)
      .map(mismatches => report.copy(mismatches = mismatches.toMap))
  }

  /**
    * Deletes the old streams of a migrated report, with their snapshots, after checking again that their new streams
    * are complete. Fails, without deleting anything, when they are not.
    */
  def deleteMigrated(report: MigrationReport): Future[Done] =
    verify(report).flatMap { checked =>
      if (!checked.verified)
        Future.failed(new IllegalStateException(s"Not all new streams are complete, nothing deleted:\n$checked"))
      else
        Source(checked.migrating.toList)
          .mapAsync(parallelism)(stream => delete(stream.persistenceId))
          .runWith(Sink.ignore)
    }

  /**
    * The streams, other than those of the type itself, whose events can still be read under the tag of an aggregate
    * type. After `deleteMigrated` there should be none. Journals that keep deleted events in their tag queries
    * (Cassandra's tag views, the in-memory journal) still return old events that carried the type's name as a tag, and
    * every event materializer of that type would handle them as well. The R2DBC journal reads types by entity type,
    * which an old stream does not have, so there it is always empty.
    */
  def oldStreamsReadableByType(types: Seq[AggregateType]): Future[Map[AggregateType, Set[String]]] =
    if (isR2dbc) Future.successful(Map.empty)
    else
      Source(types.toList)
        .mapAsync(1) { aggregateType =>
          tagJournal
            .currentEventsByTag(aggregateType.name, NoOffset)
            .map(_.persistenceId)
            .filterNot(pid => AggregateType.of(pid).contains(aggregateType))
            .runFold(Set.empty[String])(_ + _)
            .map(aggregateType -> _)
        }
        .filter(_._2.nonEmpty)
        .runWith(Sink.seq)
        .map(_.toMap)

  /**
    * Sets the offset of the given event materializers back, so they handle every event again, the copies included.
    * Only for materializers whose data is cleared and rebuilt.
    */
  def resetOffsets(store: OffsetStore, viewIdentifiers: Seq[String]): Future[Done] =
    Future.sequence(viewIdentifiers.map(store.saveOffset(_, NoOffset))).map(_ => Done)

  /**
    * Sets the offset of an event materializer to the last event of its aggregate type, so it handles only the events
    * stored after the migration. For a materializer that keeps the data it built from the old streams.
    *
    * On the R2DBC journal a reader started from an offset first reads back `backtracking.window` plus
    * `behind-current-time` before it, so the offset is set that far after the last event, and this completes only
    * once that moment has passed (about 2 minutes and 10 seconds by default): the application must not start before.
    */
  def startAtEnd(store: OffsetStore, viewIdentifier: String, aggregateType: AggregateType): Future[Done] =
    endOffsets(Seq(aggregateType)).flatMap(ends => store.saveOffset(viewIdentifier, ends(aggregateType))).map(_ => Done)

  /**
    * Sets the offsets of a combined logic to the last event of each of its aggregate types, so it handles only the events stored
    * after the migration and does not repeat its side effects for the copies. On R2DBC it waits as `startAtEnd` does.
    */
  def startCombinedLogicAtEnd(
    store: OffsetStore,
    viewIdentifier: String,
    aggregateTypes: Seq[AggregateType]
  ): Future[Done] =
    endOffsets(aggregateTypes)
      .flatMap(ends => store.saveOffset(viewIdentifier, TagOffsets(ends.map { case (t, offset) => t.name -> offset })))
      .map(_ => Done)

  private def endOffsets(types: Seq[AggregateType]): Future[Map[AggregateType, Offset]] =
    Source(types.toList)
      .mapAsync(1)(aggregateType =>
        typeJournal
          .currentEventsByAggregateType(aggregateType, NoOffset)
          .runWith(Sink.lastOption)
          .map(last => aggregateType -> last.fold[Offset](NoOffset)(_.offset))
      )
      .runWith(Sink.seq)
      .flatMap { ends =>
        val firstBacktracking =
          config
            .getDuration("pekko.persistence.r2dbc.query.backtracking.window")
            .plus(
              config.getDuration("pekko.persistence.r2dbc.query.backtracking.behind-current-time")
            )
        val moved = ends.map {
          case (aggregateType, TimestampOffset(timestamp, _, _)) =>
            aggregateType -> TimestampOffset(timestamp.plus(firstBacktracking).plus(ClockMargin), Map.empty)
          case other => other
        }
        val latest = moved.collect { case (_, TimestampOffset(timestamp, _, _)) => timestamp }.maxOption
        val wait   = latest.fold(0L)(timestamp => java.time.Duration.between(Instant.now(), timestamp).toMillis)
        if (wait <= 0) Future.successful(moved.toMap)
        else {
          log.info("Waiting {} ms until the R2DBC offsets after the migrated events have passed", wait)
          after((wait + ClockMargin.toMillis).millis)(Future.successful(moved.toMap))
        }
      }

  private def selectedStreams(): Source[String, ?] = journal.currentPersistenceIds().filter(selectStream)

  private def planAll(): Future[MigrationReport] =
    selectedStreams()
      .mapAsync(parallelism)(plan)
      .runWith(Sink.seq)
      .flatMap(withUntagged)
      .map(streams => MigrationReport(streams, conflictsOf(streams)))

  // Two old streams cannot fill one new stream, and a new stream cannot be an old one that is copied (and deleted).
  private def conflictsOf(streams: Seq[StreamMigration]): Seq[String] = {
    val migrating = streams.filter(_.migrates)
    val sources   = migrating.map(_.persistenceId).toSet
    val shared    = migrating
      .flatMap(stream => stream.targets.keys.map(_ -> stream.persistenceId))
      .groupMap(_._1)(_._2)
      .collect { case (target, from) if from.size > 1 => s"$target is the target of ${from.sorted.mkString(", ")}" }
    val copied = migrating.flatMap(stream =>
      stream.targets.keys
        .filter(sources.contains)
        .map(target =>
          s"$target is a target of ${stream.persistenceId}, " +
            "but is migrated itself"
        )
    )
    (shared ++ copied).toSeq.sorted
  }

  // On a tag-based journal, counts per stream in place the events that are not tagged with its type.
  private def withUntagged(streams: Seq[StreamMigration]): Future[Seq[StreamMigration]] = {
    val inPlace = streams.filter(_.inPlace)
    if (isR2dbc || inPlace.isEmpty) Future.successful(streams)
    else {
      val types = inPlace.flatMap(stream => AggregateType.of(stream.persistenceId)).distinct
      Source(types.toList)
        .mapAsync(1)(aggregateType =>
          tagJournal
            .currentEventsByTag(aggregateType.name, NoOffset)
            .filter(envelope => AggregateType.of(envelope.persistenceId).contains(aggregateType))
            .runFold(Map.empty[String, Long].withDefaultValue(0L))((counts, envelope) =>
              counts.updated(envelope.persistenceId, counts(envelope.persistenceId) + 1)
            )
        )
        .runFold(Map.empty[String, Long].withDefaultValue(0L))(_ ++ _)
        .map(tagged =>
          streams.map(stream =>
            if (stream.inPlace) stream.copy(untagged = math.max(0L, stream.events - tagged(stream.persistenceId)))
            else stream
          )
        )
    }
  }

  private def plan(persistenceId: String): Future[StreamMigration] =
    for {
      snapshot <- latestSnapshot(persistenceId)
      targets  <- journal
        .currentEventsByPersistenceId(persistenceId, 0L, Long.MaxValue)
        .runFold((0L, ListMap.empty[String, Long], 0L)) { case ((events, targets, unmapped), envelope) =>
          rule(persistenceId, envelope.event) match {
            case Some(target) =>
              val pid = target.persistenceId
              (events + 1, targets.updated(pid, targets.getOrElse(pid, 0L) + 1), unmapped)
            case None => (events + 1, targets, unmapped + 1)
          }
        }
    } yield {
      val (events, to, unmapped) = targets
      StreamMigration(persistenceId, events, to, unmapped, snapshot.map(_._1))
    }

  // Routes the events of the old stream to their new streams in order. A new stream already holding events (from an
  // interrupted run) gets only the events after them, once those are found to be the copies of the same events. A
  // copied snapshot is saved in the new stream after the copies of the events up to its sequence number (before the
  // first copy when those events were deleted), so recovery applies it at the same point in the history.
  private def copy(stream: StreamMigration): Future[Done] = {
    val writers = collection.mutable.Map[String, (ActorRef, Vector[Any])]()
    val routed  = collection.mutable.Map[String, Long]().withDefaultValue(0L)

    def writerFor(pid: String): Future[(ActorRef, Vector[Any])] =
      writers.get(pid).map(Future.successful).getOrElse {
        val typeName = AggregateType.of(pid).get.name
        val writer   = system.actorOf(Props(new TargetWriter(pid, typeName, tagged = !isR2dbc)))
        for {
          highest  <- (writer ? Highest).mapTo[Long]
          existing <- existingEvents(pid, highest)
        } yield {
          writers += pid -> (writer, existing)
          (writer, existing)
        }
      }

    def writeRun(pid: String, events: Vector[(Any, Set[String])]): Future[Done] =
      writerFor(pid).flatMap { case (writer, existing) =>
        val first = routed(pid) + 1
        routed(pid) += events.size
        val numbered = events.zipWithIndex.map { case (event, i) => (first + i, event) }
        numbered.collectFirst {
          case (seqNr, (event, _)) if seqNr <= existing.size && !sameEvent(existing((seqNr - 1).toInt), event) =>
            seqNr
        } match {
          case Some(seqNr) =>
            Future.failed(
              new IllegalStateException(
                s"$pid already holds an event $seqNr that is not the copy of the event it gets from " +
                  s"${stream.persistenceId}: it was not written by a migration of ${stream.persistenceId}"
              )
            )
          case None =>
            val toWrite = numbered.collect { case (seqNr, event) if seqNr > existing.size => event }
            if (toWrite.isEmpty) Future.successful(Done) else (writer ? Write(toWrite)).map(_ => Done)
        }
      }

    // The writer is then exactly at the snapshot's position, unless an earlier run already wrote past it.
    def saveSnapshot(pid: String, state: Any): Future[Done] =
      writerFor(pid).flatMap { case (writer, existing) =>
        if (routed(pid) >= existing.size) (writer ? SaveSnapshotOf(state)).map(_ => Done)
        else Future.successful(Done)
      }

    def writeBatch(batch: Seq[CopyItem]): Future[Done] = {
      val steps = batch.foldLeft(Vector.empty[CopyItem]) {
        case (acc :+ CopyEvents(pid, run), CopyEvents(nextPid, next)) if pid == nextPid =>
          acc :+ CopyEvents(pid, run ++ next)
        case (acc, item) => acc :+ item
      }
      steps.foldLeft(Future.successful(Done: Done)) {
        case (previous, CopyEvents(pid, events))  => previous.flatMap(_ => writeRun(pid, events))
        case (previous, CopySnapshot(pid, state)) => previous.flatMap(_ => saveSnapshot(pid, state))
      }
    }

    // A new stream with more events than its old stream gives it holds events of another origin.
    def checkNothingMore(): Future[Done] =
      writers.collectFirst {
        case (pid, (_, existing)) if existing.size > routed(pid) => pid -> existing.size
      } match {
        case Some((pid, size)) =>
          Future.failed(
            new IllegalStateException(
              s"$pid already holds $size events, more than the ${routed(pid)} it gets from ${stream.persistenceId}"
            )
          )
        case None => Future.successful(Done)
      }

    val snapshotToCopy =
      if (stream.copiesSnapshot) latestSnapshot(stream.persistenceId) else Future.successful(None)

    val copied = snapshotToCopy.flatMap { snapshot =>
      val events = journal
        .currentEventsByPersistenceId(stream.persistenceId, 0L, Long.MaxValue)
        .map { envelope =>
          val target = rule(stream.persistenceId, envelope.event).getOrElse(
            throw new IllegalStateException(
              s"The rule gives no target for event ${envelope.sequenceNr} of ${stream.persistenceId} anymore"
            )
          )
          envelope.sequenceNr -> CopyEvents(target.persistenceId, Vector(envelope.event -> target.tags))
        }
      val items: Source[CopyItem, ?] = snapshot match {
        case None                         => events.map(_._2)
        case Some((snapshotSeqNr, state)) =>
          val marker = CopySnapshot(stream.targets.head._1, state)
          events
            .map(Some(_))
            .concat(Source.single(None))
            .statefulMap(() => false)(
              (markerDone, item) =>
                item match {
                  case Some((seqNr, copy)) if !markerDone && seqNr > snapshotSeqNr => (true, List(marker, copy))
                  case Some((_, copy))                                             => (markerDone, List(copy))
                  case None if !markerDone                                         => (true, List(marker))
                  case None                                                        => (markerDone, Nil)
                },
              _ => None
            )
            .mapConcat(identity)
      }
      items.grouped(batchSize).mapAsync(1)(writeBatch).runWith(Sink.ignore).flatMap(_ => checkNothingMore())
    }
    copied.andThen { case _ => writers.values.foreach(_._1 ! PoisonPill) }.map { done =>
      log.info("Migrated {}", stream)
      done
    }
  }

  // The events a new stream already holds, which must be numbered 1 to highest without gaps.
  private def existingEvents(persistenceId: String, highest: Long): Future[Vector[Any]] =
    if (highest == 0L) Future.successful(Vector.empty)
    else
      journal
        .currentEventsByPersistenceId(persistenceId, 1L, highest)
        .runWith(Sink.seq)
        .flatMap { envelopes =>
          if (envelopes.map(_.sequenceNr) == (1L to highest))
            Future.successful(envelopes.map(_.event).toVector)
          else
            Future.failed(
              new IllegalStateException(
                s"$persistenceId has events up to $highest, but not all of them are readable: " +
                  envelopes.map(_.sequenceNr).mkString(", ")
              )
            )
        }

  // Equal, or serialized to the same bytes for event classes without equals.
  private def sameEvent(existing: Any, event: Any): Boolean =
    existing == event || {
      val bytes = (any: Any) => serialization.serialize(any.asInstanceOf[AnyRef]).toOption
      (bytes(existing), bytes(event)) match {
        case (Some(a), Some(b)) => java.util.Arrays.equals(a, b)
        case _                  => false
      }
    }

  private def latestSnapshot(persistenceId: String): Future[Option[(Long, Any)]] = {
    val found = Promise[Option[(Long, Any)]]()
    system.actorOf(Props(new SnapshotReader(persistenceId, found)))
    withinTimeout(found.future, s"Reading the latest snapshot of $persistenceId")
  }

  private def highestSequenceNr(persistenceId: String): Future[Long] = {
    val writer = system.actorOf(Props(new TargetWriter(persistenceId, "", tagged = false)))
    (writer ? Highest).mapTo[Long].andThen { case _ => writer ! PoisonPill }
  }

  private def delete(persistenceId: String): Future[Done] = {
    val deleted = Promise[Done]()
    system.actorOf(Props(new StreamDeleter(persistenceId, hasSnapshotStore, deleted)))
    withinTimeout(deleted.future, s"Deleting $persistenceId")
  }

  private def withinTimeout[T](future: Future[T], what: String): Future[T] =
    Future.firstCompletedOf(
      Seq(
        future,
        after(timeout.duration)(
          Future.failed[T](new TimeoutException(s"$what did not complete within ${timeout.duration}"))
        )
      )
    )
}

object AggregateTypeMigration {

  // How far the clocks of the database and the migration may differ when waiting for R2DBC offsets.
  private val ClockMargin = java.time.Duration.ofSeconds(1)

  /**
    * Every persistence id: streams the rule maps onto themselves are reported as in place.
    */
  def allStreams(persistenceId: String): Boolean = persistenceId.nonEmpty

  /**
    * The persistence ids without a `|`. Leaves out 1.0.0 streams written with a prefix such as `Order|` and typed
    * streams on `PersistenceId(type, id)`, which `allStreams` includes.
    */
  def withoutAggregateType(persistenceId: String): Boolean = AggregateType.of(persistenceId).isEmpty

  /**
    * A rule for a journal of one aggregate type per id prefix: an old stream whose persistence id starts with one of
    * the prefixes becomes `type|<rest of the id>`. Streams matching no prefix are left alone.
    */
  def byPrefix(prefixes: (String, AggregateType)*): (String, Any) => Option[MigrationTarget] =
    (persistenceId, _) =>
      prefixes.collectFirst {
        case (prefix, aggregateType) if persistenceId.startsWith(prefix) =>
          MigrationTarget(aggregateType, persistenceId.substring(prefix.length))
      }

  /**
    * A rule that decides the aggregate type by the event. The aggregate id is the persistence id after its first `|`,
    * or the whole persistence id when it has none; write a rule of your own when 1.0.0 aggregate ids contain `|`.
    */
  def byEvent(typeOf: PartialFunction[Any, AggregateType]): (String, Any) => Option[MigrationTarget] =
    (persistenceId, event) =>
      typeOf.lift(event).map(MigrationTarget(_, AggregateType.aggregateIdOf(persistenceId).getOrElse(persistenceId)))

  /**
    * Adds extra tags to the targets of a rule, e.g. those `AggregateRootActor.tags` gives the event.
    */
  def withTags(
    rule: (String, Any) => Option[MigrationTarget]
  )(tags: Any => Set[String]): (String, Any) => Option[MigrationTarget] =
    (persistenceId, event) => rule(persistenceId, event).map(target => target.copy(tags = target.tags ++ tags(event)))

  private sealed trait CopyItem
  private final case class CopyEvents(persistenceId: String, events: Vector[(Any, Set[String])]) extends CopyItem
  private final case class CopySnapshot(persistenceId: String, state: Any)                       extends CopyItem

  private case object Highest
  private final case class Write(events: Seq[(Any, Set[String])])
  private final case class SaveSnapshotOf(state: Any)

  /**
    * Writes events to one new stream: tagged with its aggregate type when `tagged`, and with their extra tags. A
    * write the journal rejects fails the copy.
    *
    * It recovers the events the stream already holds (from an interrupted run) and ignores them, without a snapshot.
    * Not `Recovery.none`: the Cassandra journal restores a stream's position per tag during recovery, and without it
    * numbers the tag rows of the next copies from 1 again, so its tag queries drop them as duplicates.
    */
  private class TargetWriter(override val persistenceId: String, typeName: String, tagged: Boolean)
      extends PersistentActor {
    override def recovery: Recovery = Recovery(fromSnapshot = SnapshotSelectionCriteria.None)

    override def receiveRecover: Receive = { case _ => () }

    private var snapshotFor: Option[ActorRef] = None
    private var rejected: Option[Throwable]   = None

    override def receiveCommand: Receive = {
      case Highest =>
        sender() ! lastSequenceNr
      case Write(events) =>
        val replyTo = sender()
        persistAll(events.map { case (event, extraTags) =>
          val tags = if (tagged) extraTags + typeName else extraTags
          if (tags.isEmpty) event else Tagged(event, tags)
        })(_ => ())
        deferAsync(()) { _ =>
          rejected match {
            case Some(cause) =>
              replyTo ! Status.Failure(
                new IllegalStateException(s"The journal rejected a copy for $persistenceId", cause)
              )
              context.stop(self)
            case None => replyTo ! lastSequenceNr
          }
        }
      case SaveSnapshotOf(state) =>
        snapshotFor = Some(sender())
        saveSnapshot(state)
      case SaveSnapshotSuccess(_) =>
        snapshotFor.foreach(_ ! Done)
        snapshotFor = None
      case SaveSnapshotFailure(_, cause) =>
        snapshotFor.foreach(_ ! Status.Failure(cause))
        snapshotFor = None
    }

    override protected def onPersistRejected(cause: Throwable, event: Any, seqNr: Long): Unit = {
      super.onPersistRejected(cause, event, seqNr)
      if (rejected.isEmpty) rejected = Some(cause)
    }
  }

  /** Reads the latest snapshot of a stream, without replaying its events. */
  private class SnapshotReader(override val persistenceId: String, found: Promise[Option[(Long, Any)]])
      extends PersistentActor {
    override def recovery: Recovery = Recovery(fromSnapshot = SnapshotSelectionCriteria.Latest, replayMax = 0L)

    private var snapshot: Option[(Long, Any)] = None

    override def receiveRecover: Receive = {
      case SnapshotOffer(metadata, state) => snapshot = Some(metadata.sequenceNr -> state)
      case RecoveryCompleted              =>
        found.trySuccess(snapshot)
        context.stop(self)
      case _ => ()
    }

    override def receiveCommand: Receive = { case _ => () }

    override protected def onRecoveryFailure(cause: Throwable, event: Option[Any]): Unit = {
      found.tryFailure(cause)
      super.onRecoveryFailure(cause, event)
    }
  }

  /** Deletes the events of a stream, and its snapshots when there is a snapshot store. */
  private class StreamDeleter(override val persistenceId: String, withSnapshots: Boolean, deleted: Promise[Done])
      extends PersistentActor {
    override def recovery: Recovery = Recovery.none

    override def receiveRecover: Receive = { case RecoveryCompleted => deleteMessages(Long.MaxValue) }

    override def receiveCommand: Receive = {
      case DeleteMessagesSuccess(_) =>
        if (withSnapshots) deleteSnapshots(SnapshotSelectionCriteria.Latest) else done()
      case DeleteSnapshotsSuccess(_) =>
        done()
      case DeleteMessagesFailure(cause, _) =>
        failed(cause)
      case DeleteSnapshotsFailure(_, cause) =>
        failed(cause)
    }

    override protected def onRecoveryFailure(cause: Throwable, event: Option[Any]): Unit = {
      deleted.tryFailure(cause)
      super.onRecoveryFailure(cause, event)
    }

    private def done(): Unit = {
      deleted.trySuccess(Done)
      context.stop(self)
    }

    private def failed(cause: Throwable): Unit = {
      deleted.tryFailure(cause)
      context.stop(self)
    }
  }
}
