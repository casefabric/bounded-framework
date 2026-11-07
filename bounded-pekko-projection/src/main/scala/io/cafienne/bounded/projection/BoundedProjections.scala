/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.projection

import org.apache.pekko.Done
import org.apache.pekko.actor.typed.{ActorRef, ActorSystem}
import org.apache.pekko.actor.typed.eventstream.EventStream
import org.apache.pekko.cluster.sharding.typed.ShardedDaemonProcessSettings
import org.apache.pekko.cluster.sharding.typed.scaladsl.ShardedDaemonProcess
import org.apache.pekko.persistence.query.Offset
import org.apache.pekko.persistence.query.typed.{EventEnvelope => SliceEnvelope}
import org.apache.pekko.projection.{
  HandlerRecoveryStrategy,
  Projection,
  ProjectionBehavior,
  ProjectionId,
  StatusObserver
}
import org.apache.pekko.projection.eventsourced.EventEnvelope
import org.apache.pekko.projection.eventsourced.scaladsl.EventSourcedProvider
import org.apache.pekko.projection.scaladsl.{Handler, SourceProvider}
import io.cafienne.bounded.eventmaterializers.EventProcessed
import io.cafienne.bounded.pekko.persistence.ReadJournalProvider
import io.cafienne.bounded.readside.{ProjectionDown, ProjectionFailure, ProjectionProgress, ReadSideRegistry}

import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.Future

/**
  * Runs Bounded read models, built with the factory of their store, as Pekko Projections: on this node, or once in
  * a cluster.
  *
  * {{{
  * val projection = BoundedJdbcProjection.exactlyOnce(system, overview, () => new MySession(dataSource.getConnection)) {
  *   (session, event) => ...
  * }
  * BoundedProjections.runInCluster(system, "read-models", List(projection))
  * }}}
  */
object BoundedProjections {

  /**
    * Runs a projection on this node, as a system actor. Stop it with `ProjectionBehavior.Stop`.
    */
  def runLocally(system: ActorSystem[?], projection: Projection[?]): ActorRef[ProjectionBehavior.Command] =
    system.systemActorOf(ProjectionBehavior(projection), s"projection-${projection.projectionId.id}")

  /**
    * Runs the projections once in the cluster, spread over its nodes with a `ShardedDaemonProcess`: each runs on one
    * node at a time, and moves to another node when its node leaves. Every node calls this with the same `name` and
    * the same projections (by projection id; their order does not matter).
    */
  def runInCluster(system: ActorSystem[?], name: String, projections: Seq[Projection[?]]): Unit = {
    require(projections.nonEmpty, "There are no projections to run")
    val ids = projections.map(_.projectionId.id)
    require(ids.distinct.size == ids.size, s"The projection ids are not unique: ${ids.mkString(", ")}")
    val ordered = projections.sortBy(_.projectionId.id).toVector
    ShardedDaemonProcess(system).init[ProjectionBehavior.Command](
      name,
      ordered.size,
      index => ProjectionBehavior(ordered(index)),
      ShardedDaemonProcessSettings(system),
      Some(ProjectionBehavior.Stop)
    )
  }

  /** Whether the configured journal is the R2DBC journal. */
  private[projection] def isR2dbcJournal(system: ActorSystem[?]): Boolean =
    ReadJournalProvider.isR2dbcJournal(configuredJournal(system))

  private[projection] def queryPluginId(system: ActorSystem[?]): String =
    ReadJournalProvider.queryPluginId(configuredJournal(system))

  /**
    * The events of the aggregate type by its tag, for a store whose offset store cannot hold the offsets of the R2DBC
    * journal (which has no queries by tag anyway).
    */
  private[projection] def eventsByTag(
    system: ActorSystem[?],
    projection: EventProjection,
    store: String
  ): SourceProvider[Offset, EventEnvelope[Any]] = {
    require(
      !isR2dbcJournal(system),
      s"$store cannot store the offsets of the R2DBC journal; run ${projection.projectionName} with BoundedR2dbcProjection"
    )
    EventSourcedProvider.eventsByTag[Any](system, queryPluginId(system), projection.aggregateType.name)
  }

  /** The events of the aggregate type on the R2DBC journal, by its entity type over the given slices. */
  private[projection] def eventsBySlices(
    system: ActorSystem[?],
    projection: EventProjection,
    slices: SliceRange
  ): SourceProvider[Offset, SliceEnvelope[Any]] =
    EventSourcedProvider.eventsBySlices[Any](
      system,
      queryPluginId(system),
      projection.aggregateType.name,
      slices.min,
      slices.max
    )

  /**
    * The status observer of every Bounded projection. When `bounded.eventmaterializers.publish` is on it publishes
    * on the event stream, like the event materializers do:
    *  - after an event is processed (for an exactly-once projection: once it is committed) an `EventProcessed`
    *    when the filter accepts the event, and a `ProjectionProgress` for every event;
    *  - a `ProjectionFailure` when the handler fails on an event, final when the recovery strategy gives up on it;
    *  - a `ProjectionDown` when the projection's stream fails.
    * It also registers the projection with the `ReadSideRegistry`, so it can be waited for.
    */
  private[projection] def publishEventProcessed[Envelope](system: ActorSystem[?], projection: EventProjection)(
    implicit envelope: ProjectionEnvelope[Envelope]
  ): StatusObserver[Envelope] = {
    val config  = system.settings.config
    val publish = config.hasPath("bounded.eventmaterializers.publish") &&
      config.getBoolean("bounded.eventmaterializers.publish")
    val readSideId = projection.readSideId
    if (publish) ReadSideRegistry(system.classicSystem).register(readSideId)

    def emit(message: AnyRef): Unit = if (publish) system.eventStream ! EventStream.Publish(message)

    new StatusObserver[Envelope] {
      // The failed attempts per event, to know when the recovery strategy gives up on it
      private val attempts = new ConcurrentHashMap[(String, Long), Integer]()

      override def started(projectionId: ProjectionId): Unit                       = ()
      override def stopped(projectionId: ProjectionId): Unit                       = ()
      override def beforeProcess(projectionId: ProjectionId, env: Envelope): Unit  = ()
      override def offsetProgress(projectionId: ProjectionId, env: Envelope): Unit = ()

      override def failed(projectionId: ProjectionId, cause: Throwable): Unit = {
        attempts.clear()
        emit(ProjectionDown(readSideId, cause))
      }

      override def error(
        projectionId: ProjectionId,
        env: Envelope,
        cause: Throwable,
        recoveryStrategy: HandlerRecoveryStrategy
      ): Unit = {
        val key     = (envelope.persistenceId(env), envelope.sequenceNr(env))
        val attempt = attempts.merge(key, 1, (a: Integer, b: Integer) => Integer.valueOf(a + b)).intValue()
        val isFinal = attempt > retriesOf(recoveryStrategy)
        if (isFinal) { attempts.remove(key); () }
        emit(ProjectionFailure(readSideId, key._1, key._2, cause, isFinal))
      }

      override def afterProcess(projectionId: ProjectionId, env: Envelope): Unit = {
        val event         = envelope.event(env)
        val persistenceId = envelope.persistenceId(env)
        val sequenceNr    = envelope.sequenceNr(env)
        if (!attempts.isEmpty) { attempts.remove((persistenceId, sequenceNr)); () }
        if (projection.accepts(event)) {
          emit(EventProcessed(projection.materializerId, envelope.offset(env), persistenceId, sequenceNr, event))
        }
        emit(ProjectionProgress(readSideId, persistenceId, sequenceNr))
      }
    }
  }

  /**
    * The number of retries after the first attempt of a recovery strategy: 0 for `fail` and `skip`, n for
    * `retryAndFail(n, _)` and `retryAndSkip(n, _)`. Pekko keeps the strategy classes internal, so they are read as
    * the case classes they are; an unknown strategy is taken to retry without end, so its failures are never final.
    */
  private[projection] def retriesOf(strategy: HandlerRecoveryStrategy): Int = strategy match {
    case p: Product if p.productPrefix == "Fail" || p.productPrefix == "Skip" => 0
    case p: Product
        if (p.productPrefix == "RetryAndFail" || p.productPrefix == "RetryAndSkip") && p.productArity == 2 =>
      p.productElement(0) match {
        case retries: Int => retries
        case _            => Int.MaxValue
      }
    case _ => Int.MaxValue
  }

  private def configuredJournal(system: ActorSystem[?]): String =
    system.settings.config.getString("pekko.persistence.journal.plugin")
}

/**
  * The parts of the handlers that every store shares: events the filter rejects skip the handler function, and their
  * offset is stored as usual. A group is handed over without its rejected events, and not at all when none is left.
  */
private[projection] object BoundedHandlers {

  def accepted[Envelope](projection: EventProjection, envelopes: Seq[Envelope])(
    implicit envelope: ProjectionEnvelope[Envelope]
  ): Seq[Any] = envelopes.map(envelope.event).filter(projection.accepts)

  def async[Envelope](projection: EventProjection)(handle: Any => Future[Done])(
    implicit envelope: ProjectionEnvelope[Envelope]
  ): Handler[Envelope] = new Handler[Envelope] {
    override def process(env: Envelope): Future[Done] = {
      val event = envelope.event(env)
      if (projection.accepts(event)) handle(event) else Future.successful(Done)
    }
  }

  def groupedAsync[Envelope](projection: EventProjection)(handle: Seq[Any] => Future[Done])(
    implicit envelope: ProjectionEnvelope[Envelope]
  ): Handler[Seq[Envelope]] = new Handler[Seq[Envelope]] {
    override def process(envelopes: Seq[Envelope]): Future[Done] = {
      val events = accepted(projection, envelopes)
      if (events.nonEmpty) handle(events) else Future.successful(Done)
    }
  }
}
