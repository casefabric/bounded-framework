/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.projection

import org.apache.pekko.Done
import org.apache.pekko.actor.typed.{ActorRef, ActorSystem}
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
import io.cafienne.bounded.aggregate.AggregateType
import io.cafienne.bounded.eventmaterializers.EventProcessed
import io.cafienne.bounded.pekko.persistence.ReadJournalProvider

import scala.concurrent.Future

/**
  * Runs Bounded read models as Pekko Projections: the source of an aggregate type's events, the at-least-once
  * handler, publishing `EventProcessed`, and running projections on one node or once in a cluster.
  *
  * {{{
  * val projection = JdbcProjection
  *   .exactlyOnce(
  *     overview.projectionId,
  *     BoundedProjections.eventsByAggregateType(system, overview.aggregateType),
  *     () => new MySession(dataSource.getConnection),
  *     () => BoundedJdbcHandler(overview)((session: MySession, event: Any) => ...)
  *   )
  *   .withStatusObserver(BoundedProjections.publishEventProcessed(system, overview))
  * BoundedProjections.runInCluster(system, "read-models", List(projection))
  * }}}
  */
object BoundedProjections {

  /**
    * The events of an aggregate type by its tag, on the journals that tag events with the type (JDBC, Cassandra,
    * LevelDB, `inmemory-journal`), from the read journal of the configured journal plugin.
    */
  def eventsByAggregateType(
    system: ActorSystem[?],
    aggregateType: AggregateType
  ): SourceProvider[Offset, EventEnvelope[Any]] = {
    val journal = configuredJournal(system)
    require(
      !ReadJournalProvider.isR2dbcJournal(journal),
      "The R2DBC journal has no queries by tag; use eventsByAggregateTypeSlices"
    )
    EventSourcedProvider.eventsByTag[Any](system, ReadJournalProvider.queryPluginId(journal), aggregateType.name)
  }

  /**
    * The events of an aggregate type on the R2DBC journal, by its entity type over all slices. Use it with
    * `R2dbcProjection`, whose offset store keeps the offsets per slice.
    */
  def eventsByAggregateTypeSlices(
    system: ActorSystem[?],
    aggregateType: AggregateType
  ): SourceProvider[Offset, SliceEnvelope[Any]] = {
    val journal  = configuredJournal(system)
    val pluginId = ReadJournalProvider.queryPluginId(journal)
    val slices   = EventSourcedProvider.sliceRanges(system, pluginId, 1).head
    EventSourcedProvider.eventsBySlices[Any](system, pluginId, aggregateType.name, slices.min, slices.max)
  }

  /**
    * Publishes an `EventProcessed` on the event stream after an event is processed (for an exactly-once projection:
    * once it is committed), when `bounded.eventmaterializers.publish` is on, like the event materializers do. Events
    * the filter rejects are not published.
    */
  def publishEventProcessed[Envelope](system: ActorSystem[?], projection: EventProjection)(
    implicit envelope: ProjectionEnvelope[Envelope]
  ): StatusObserver[Envelope] = {
    val config  = system.settings.config
    val publish = config.hasPath("bounded.eventmaterializers.publish") &&
      config.getBoolean("bounded.eventmaterializers.publish")
    new StatusObserver[Envelope] {
      override def started(projectionId: ProjectionId): Unit                       = ()
      override def failed(projectionId: ProjectionId, cause: Throwable): Unit      = ()
      override def stopped(projectionId: ProjectionId): Unit                       = ()
      override def beforeProcess(projectionId: ProjectionId, env: Envelope): Unit  = ()
      override def offsetProgress(projectionId: ProjectionId, env: Envelope): Unit = ()
      override def error(
        projectionId: ProjectionId,
        env: Envelope,
        cause: Throwable,
        recoveryStrategy: HandlerRecoveryStrategy
      ): Unit = ()

      override def afterProcess(projectionId: ProjectionId, env: Envelope): Unit = {
        val event = envelope.event(env)
        if (publish && projection.accepts(event)) {
          system.eventStream ! org.apache.pekko.actor.typed.eventstream.EventStream.Publish(
            EventProcessed(
              projection.materializerId,
              envelope.offset(env),
              envelope.persistenceId(env),
              envelope.sequenceNr(env),
              event
            )
          )
        }
      }
    }
  }

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

  private def configuredJournal(system: ActorSystem[?]): String =
    system.settings.config.getString("pekko.persistence.journal.plugin")
}

/**
  * The at-least-once handler of a projection (for `atLeastOnce` and `atLeastOnceAsync` of every projection style,
  * and Cassandra's): events the filter rejects skip `handle`, and their offset is stored as usual. After a failure
  * or a restart an event can be handled again, so `handle` must be idempotent.
  */
object BoundedHandler {
  def apply[Envelope](projection: EventProjection)(handle: Any => Future[Done])(
    implicit envelope: ProjectionEnvelope[Envelope]
  ): Handler[Envelope] = new Handler[Envelope] {
    override def process(env: Envelope): Future[Done] = {
      val event = envelope.event(env)
      if (projection.accepts(event)) handle(event) else Future.successful(Done)
    }
  }

  /** The handler for the envelopes by tag, from `BoundedProjections.eventsByAggregateType`. */
  def byTag(projection: EventProjection)(handle: Any => Future[Done]): Handler[EventEnvelope[Any]] =
    apply[EventEnvelope[Any]](projection)(handle)

  /** The handler for the envelopes by slices, from `BoundedProjections.eventsByAggregateTypeSlices`. */
  def bySlices(projection: EventProjection)(handle: Any => Future[Done]): Handler[SliceEnvelope[Any]] =
    apply[SliceEnvelope[Any]](projection)(handle)

}
