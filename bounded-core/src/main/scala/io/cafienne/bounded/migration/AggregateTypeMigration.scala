/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.migration

import org.apache.pekko.Done
import org.apache.pekko.actor.{ActorRef, ActorSystem, PoisonPill, Props, Status}
import org.apache.pekko.event.Logging
import org.apache.pekko.pattern.ask
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
import org.apache.pekko.persistence.query.{NoOffset, PersistenceQuery}
import org.apache.pekko.persistence.query.scaladsl.{
  CurrentEventsByPersistenceIdQuery,
  CurrentEventsByTagQuery,
  CurrentPersistenceIdsQuery,
  ReadJournal
}
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.apache.pekko.util.Timeout
import io.cafienne.bounded.aggregate.AggregateType
import io.cafienne.bounded.eventmaterializers.offsetstores.OffsetStore
import io.cafienne.bounded.pekko.persistence.ReadJournalProvider

import scala.collection.immutable.ListMap
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}

/**
  * Where an event of an old stream goes: the stream `aggregateType|aggregateId`.
  */
final case class MigrationTarget(aggregateType: AggregateType, aggregateId: String) {
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
  */
final case class StreamMigration(
  persistenceId: String,
  events: Long,
  targets: ListMap[String, Long],
  unmapped: Long,
  snapshotSequenceNr: Option[Long]
) {

  /** Whether the stream is copied: all its events have a target, and it has at least one. */
  def migrates: Boolean = unmapped == 0 && targets.nonEmpty

  /** Whether the events go to more than one new stream; the snapshot is then dropped. */
  def split: Boolean = targets.size > 1

  /** Whether the latest snapshot is copied: the stream migrates and is not split. */
  def copiesSnapshot: Boolean = migrates && !split && snapshotSequenceNr.isDefined

  override def toString: String =
    if (!migrates) s"$persistenceId: left alone ($events events, $unmapped without target)"
    else {
      val to       = targets.map { case (pid, count) => s"$pid ($count)" }.mkString(", ")
      val snapshot = snapshotSequenceNr.fold("no snapshot")(nr =>
        if (copiesSnapshot) s"snapshot at $nr copied" else s"snapshot at $nr dropped (split)"
      )
      s"$persistenceId: $events events to $to, $snapshot"
    }
}

/**
  * The streams a migration looked at, and for a run that copied, the new streams whose event count does not match.
  */
final case class MigrationReport(streams: Seq[StreamMigration], mismatches: Map[String, (Long, Long)] = Map.empty) {
  def migrating: Seq[StreamMigration] = streams.filter(_.migrates)
  def leftAlone: Seq[StreamMigration] = streams.filterNot(_.migrates)

  /** Whether every new stream holds the number of events planned for it. */
  def verified: Boolean = mismatches.isEmpty

  override def toString: String =
    (s"${migrating.size} streams to migrate, ${leftAlone.size} left alone" +:
      (streams.map(_.toString) ++ mismatches.map { case (pid, (expected, found)) =>
        s"MISMATCH $pid: expected $expected events, found $found"
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
  * get new write timestamps, and event metadata is not copied.
  *
  * The application must not run while migrating: an event written to an old stream after it was copied is lost.
  * The order:
  *  1. stop the application;
  *  1. `dryRun()` and check the report: which streams go where, which are split, which are left alone;
  *  1. `migrate()`, which can be run again after an interruption and then continues where it stopped;
  *  1. check that the returned report is `verified`;
  *  1. `deleteMigrated(report)`: old streams and their snapshots are deleted. Until then old and new events exist side
  *     by side; on a tag-based journal an old event can carry the same tag (e.g. from an event adapter), so no
  *     materializer may replay before this step;
  *  1. `oldStreamsReadableByType(...)` for every aggregate type must be empty. Cassandra keeps deleted events in its
  *     tag views: when old events were tagged with the name of their new type, clean those with the plugin's
  *     reconciliation tool;
  *  1. `resetOffsets(...)` for every event materializer: the copies have new offsets, so each replays once;
  *  1. start the new version of the application.
  *
  * @param system the actor system with the journal and snapshot store to migrate
  * @param rule the new stream of an event, given the old persistence id and the event; None for an event that has
  *             none, which leaves its whole stream alone. Events of one old stream with different targets split it.
  * @param selectStream which old persistence ids to look at; by default those without an aggregate type
  * @param batchSize the number of events written at once
  * @param parallelism the number of old streams migrated at the same time
  */
class AggregateTypeMigration(
  system: ActorSystem,
  rule: (String, Any) => Option[MigrationTarget],
  selectStream: String => Boolean = AggregateTypeMigration.withoutAggregateType,
  batchSize: Int = 100,
  parallelism: Int = 4
)(implicit timeout: Timeout = Timeout(30.seconds)) {
  import AggregateTypeMigration._

  require(batchSize > 0, "batchSize must be positive")
  require(parallelism > 0, "parallelism must be positive")

  private implicit val actorSystem: ActorSystem = system
  private implicit val ec: ExecutionContext     = system.dispatcher
  private val log                               = Logging(system, classOf[AggregateTypeMigration])

  private val configuredJournal = system.settings.config.getString("pekko.persistence.journal.plugin")

  private val journal: ReadJournal with CurrentPersistenceIdsQuery with CurrentEventsByPersistenceIdQuery =
    PersistenceQuery(system)
      .readJournalFor[ReadJournal with CurrentPersistenceIdsQuery with CurrentEventsByPersistenceIdQuery](
        ReadJournalProvider.queryPluginId(configuredJournal)
      )

  /**
    * What `migrate()` would do, without writing anything.
    */
  def dryRun(): Future[MigrationReport] =
    selectedStreams().mapAsync(parallelism)(plan).runWith(Sink.seq).map(MigrationReport(_))

  /**
    * Copies every selected stream whose events all have a target, and verifies the new streams. Streams copied by an
    * earlier, interrupted run are completed, not copied again.
    */
  def migrate(): Future[MigrationReport] =
    selectedStreams()
      .mapAsync(parallelism)(plan)
      .mapAsync(parallelism)(stream =>
        if (stream.migrates) copy(stream).map(_ => stream) else Future.successful(stream)
      )
      .runWith(Sink.seq)
      .flatMap(streams => verify(MigrationReport(streams)))

  /**
    * Checks that every new stream of the report holds the planned number of events.
    */
  def verify(report: MigrationReport): Future[MigrationReport] = {
    val expected = report.migrating.flatMap(_.targets).groupMapReduce(_._1)(_._2)(_ + _)
    Source(expected.toList)
      .mapAsync(parallelism) { case (pid, count) => highestSequenceNr(pid).map(found => pid -> (count, found)) }
      .filter { case (_, (count, found)) => count != found }
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
    if (ReadJournalProvider.isR2dbcJournal(configuredJournal)) Future.successful(Map.empty)
    else {
      val byTag = PersistenceQuery(system)
        .readJournalFor[ReadJournal with CurrentEventsByTagQuery](ReadJournalProvider.queryPluginId(configuredJournal))
      Source(types.toList)
        .mapAsync(1) { aggregateType =>
          byTag
            .currentEventsByTag(aggregateType.name, NoOffset)
            .map(_.persistenceId)
            .filterNot(pid => AggregateType.of(pid).contains(aggregateType))
            .runFold(Set.empty[String])(_ + _)
            .map(aggregateType -> _)
        }
        .filter(_._2.nonEmpty)
        .runWith(Sink.seq)
        .map(_.toMap)
    }

  /**
    * Sets the offset of the given event materializers back, so they replay the migrated events once.
    */
  def resetOffsets(store: OffsetStore, viewIdentifiers: Seq[String]): Future[Done] =
    Future.sequence(viewIdentifiers.map(store.saveOffset(_, NoOffset))).map(_ => Done)

  private def selectedStreams(): Source[String, ?] = journal.currentPersistenceIds().filter(selectStream)

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
  // interrupted run) gets only the events after them. A copied snapshot is saved in the new stream after the copies of
  // the events up to its sequence number (before the first copy when those events were deleted), so recovery applies
  // it at the same point in the history.
  private def copy(stream: StreamMigration): Future[Done] = {
    val writers = collection.mutable.Map[String, (ActorRef, Long)]()
    val routed  = collection.mutable.Map[String, Long]().withDefaultValue(0L)

    def writerFor(pid: String): Future[(ActorRef, Long)] =
      writers.get(pid).map(Future.successful).getOrElse {
        val tag    = AggregateType.of(pid).get.name
        val writer = system.actorOf(Props(new TargetWriter(pid, tag)))
        (writer ? Highest).mapTo[Long].map { highest =>
          writers += pid -> (writer, highest)
          (writer, highest)
        }
      }

    def writeRun(pid: String, events: Vector[Any]): Future[Done] =
      writerFor(pid).flatMap { case (writer, highest) =>
        val first = routed(pid) + 1
        routed(pid) += events.size
        val toWrite = events.zipWithIndex.collect { case (event, i) if first + i > highest => event }
        if (toWrite.isEmpty) Future.successful(Done) else (writer ? Write(toWrite)).map(_ => Done)
      }

    // The writer is then exactly at the snapshot's position, unless an earlier run already wrote past it.
    def saveSnapshot(pid: String, state: Any): Future[Done] =
      writerFor(pid).flatMap { case (writer, highest) =>
        if (routed(pid) >= highest) (writer ? SaveSnapshotOf(state)).map(_ => Done) else Future.successful(Done)
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
          envelope.sequenceNr -> CopyEvents(target.persistenceId, Vector(envelope.event))
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
      items.grouped(batchSize).mapAsync(1)(writeBatch).runWith(Sink.ignore)
    }
    copied.andThen { case _ => writers.values.foreach(_._1 ! PoisonPill) }.map { done =>
      log.info("Migrated {}", stream)
      done
    }
  }

  private def latestSnapshot(persistenceId: String): Future[Option[(Long, Any)]] = {
    val found = Promise[Option[(Long, Any)]]()
    system.actorOf(Props(new SnapshotReader(persistenceId, found)))
    found.future
  }

  private def highestSequenceNr(persistenceId: String): Future[Long] = {
    val writer = system.actorOf(Props(new TargetWriter(persistenceId, "")))
    (writer ? Highest).mapTo[Long].andThen { case _ => writer ! PoisonPill }
  }

  private def delete(persistenceId: String): Future[Done] = {
    val deleted = Promise[Done]()
    system.actorOf(Props(new StreamDeleter(persistenceId, deleted)))
    deleted.future
  }
}

object AggregateTypeMigration {

  /**
    * The persistence ids that are not `type|id` yet.
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
    * A rule that decides the aggregate type by the event: the persistence id stays the aggregate id.
    */
  def byEvent(typeOf: PartialFunction[Any, AggregateType]): (String, Any) => Option[MigrationTarget] =
    (persistenceId, event) => typeOf.lift(event).map(MigrationTarget(_, persistenceId))

  private sealed trait CopyItem
  private final case class CopyEvents(persistenceId: String, events: Vector[Any]) extends CopyItem
  private final case class CopySnapshot(persistenceId: String, state: Any)        extends CopyItem

  private case object Highest
  private final case class Write(events: Seq[Any])
  private final case class SaveSnapshotOf(state: Any)

  /** Writes events to one new stream, tagged with its aggregate type. Does not replay what the stream holds. */
  private class TargetWriter(override val persistenceId: String, tag: String) extends PersistentActor {
    override def recovery: Recovery = Recovery.none

    override def receiveRecover: Receive = { case _ => () }

    private var snapshotFor: Option[ActorRef] = None

    override def receiveCommand: Receive = {
      case Highest =>
        sender() ! lastSequenceNr
      case Write(events) =>
        val replyTo = sender()
        persistAll(events.map(event => Tagged(event, Set(tag))))(_ => ())
        deferAsync(())(_ => replyTo ! lastSequenceNr)
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

  /** Deletes the events and snapshots of a stream. */
  private class StreamDeleter(override val persistenceId: String, deleted: Promise[Done]) extends PersistentActor {
    override def recovery: Recovery = Recovery.none

    override def receiveRecover: Receive = { case RecoveryCompleted => deleteMessages(Long.MaxValue) }

    override def receiveCommand: Receive = {
      case DeleteMessagesSuccess(_) =>
        deleteSnapshots(SnapshotSelectionCriteria.Latest)
      case DeleteSnapshotsSuccess(_) =>
        deleted.trySuccess(Done)
        context.stop(self)
      case DeleteMessagesFailure(cause, _) =>
        deleted.tryFailure(cause)
        context.stop(self)
      case DeleteSnapshotsFailure(_, cause) =>
        deleted.tryFailure(cause)
        context.stop(self)
    }
  }
}
