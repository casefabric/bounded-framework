/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.projection

import org.apache.pekko.Done
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.persistence.query.Offset
import org.apache.pekko.projection.eventsourced.EventEnvelope
import org.apache.pekko.projection.jdbc.JdbcSession
import org.apache.pekko.projection.jdbc.scaladsl.{JdbcHandler, JdbcProjection}
import org.apache.pekko.projection.scaladsl.{AtLeastOnceProjection, ExactlyOnceProjection, GroupedProjection}

import scala.concurrent.Future

/**
  * A Bounded read model as a `JdbcProjection` (needs `pekko-projection-jdbc`), with its offsets in the projection's
  * JDBC offset store. Each method calls the `JdbcProjection` method of the same name. It reads the events of the
  * aggregate type by tag, so it runs on the tag-based journals, not on R2DBC (use `BoundedR2dbcProjection` there).
  *
  * The handler function gets the events; events the filter of the `EventProjection` rejects skip it, and their offset
  * is stored. The projection publishes `EventProcessed`, and is then given to `adjust`, which can change it with the
  * methods of the Pekko projection (`withRecoveryStrategy`, `withRestartBackoff`, `withSaveOffset`, `withGroup`,
  * ...). A `withStatusObserver` in `adjust` replaces the `EventProcessed` observer.
  */
object BoundedJdbcProjection {
  private val store = "JdbcProjection"

  /**
    * `handle` runs in the transaction that stores the offset: its writes on the session and the offset are committed
    * together, or not at all.
    */
  def exactlyOnce[S <: JdbcSession](
    system: ActorSystem[?],
    projection: EventProjection,
    sessionFactory: () => S,
    adjust: ExactlyOnceProjection[Offset, EventEnvelope[Any]] => ExactlyOnceProjection[Offset, EventEnvelope[Any]] =
      identity
  )(handle: (S, Any) => Unit): ExactlyOnceProjection[Offset, EventEnvelope[Any]] =
    adjust(
      JdbcProjection
        .exactlyOnce(
          projection.projectionId,
          BoundedProjections.eventsByTag(system, projection, store),
          sessionFactory,
          () => handler(projection)(handle)
        )(system)
        .withStatusObserver(BoundedProjections.publishEventProcessed[EventEnvelope[Any]](system, projection))
    )

  /**
    * `handle` runs in a transaction on the session; the offsets are stored after the events, in batches, so `handle`
    * must be idempotent.
    */
  def atLeastOnce[S <: JdbcSession](
    system: ActorSystem[?],
    projection: EventProjection,
    sessionFactory: () => S,
    adjust: AtLeastOnceProjection[Offset, EventEnvelope[Any]] => AtLeastOnceProjection[Offset, EventEnvelope[Any]] =
      identity
  )(handle: (S, Any) => Unit): AtLeastOnceProjection[Offset, EventEnvelope[Any]] =
    adjust(
      JdbcProjection
        .atLeastOnce(
          projection.projectionId,
          BoundedProjections.eventsByTag(system, projection, store),
          sessionFactory,
          () => handler(projection)(handle)
        )(system)
        .withStatusObserver(BoundedProjections.publishEventProcessed[EventEnvelope[Any]](system, projection))
    )

  /** The offsets are stored after the events, in batches; `handle` must be idempotent. */
  def atLeastOnceAsync[S <: JdbcSession](
    system: ActorSystem[?],
    projection: EventProjection,
    sessionFactory: () => S,
    adjust: AtLeastOnceProjection[Offset, EventEnvelope[Any]] => AtLeastOnceProjection[Offset, EventEnvelope[Any]] =
      identity
  )(handle: Any => Future[Done]): AtLeastOnceProjection[Offset, EventEnvelope[Any]] =
    adjust(
      JdbcProjection
        .atLeastOnceAsync(
          projection.projectionId,
          BoundedProjections.eventsByTag(system, projection, store),
          sessionFactory,
          () => BoundedHandlers.async[EventEnvelope[Any]](projection)(handle)
        )(system)
        .withStatusObserver(BoundedProjections.publishEventProcessed[EventEnvelope[Any]](system, projection))
    )

  /**
    * `handle` gets the events in groups (size and time set with `withGroup`) and runs in the transaction that stores
    * the offset of the group.
    */
  def groupedWithin[S <: JdbcSession](
    system: ActorSystem[?],
    projection: EventProjection,
    sessionFactory: () => S,
    adjust: GroupedProjection[Offset, EventEnvelope[Any]] => GroupedProjection[Offset, EventEnvelope[Any]] = identity
  )(handle: (S, Seq[Any]) => Unit): GroupedProjection[Offset, EventEnvelope[Any]] =
    adjust(
      JdbcProjection
        .groupedWithin(
          projection.projectionId,
          BoundedProjections.eventsByTag(system, projection, store),
          sessionFactory,
          () => groupedHandler(projection)(handle)
        )(system)
        .withStatusObserver(BoundedProjections.publishEventProcessed[EventEnvelope[Any]](system, projection))
    )

  /** `handle` gets the events in groups; the offset of a group is stored after it, so `handle` must be idempotent. */
  def groupedWithinAsync[S <: JdbcSession](
    system: ActorSystem[?],
    projection: EventProjection,
    sessionFactory: () => S,
    adjust: GroupedProjection[Offset, EventEnvelope[Any]] => GroupedProjection[Offset, EventEnvelope[Any]] = identity
  )(handle: Seq[Any] => Future[Done]): GroupedProjection[Offset, EventEnvelope[Any]] =
    adjust(
      JdbcProjection
        .groupedWithinAsync(
          projection.projectionId,
          BoundedProjections.eventsByTag(system, projection, store),
          sessionFactory,
          () => BoundedHandlers.groupedAsync[EventEnvelope[Any]](projection)(handle)
        )(system)
        .withStatusObserver(BoundedProjections.publishEventProcessed[EventEnvelope[Any]](system, projection))
    )

  private def handler[S <: JdbcSession](projection: EventProjection)(
    handle: (S, Any) => Unit
  ): JdbcHandler[EventEnvelope[Any], S] =
    new JdbcHandler[EventEnvelope[Any], S] {
      override def process(session: S, envelope: EventEnvelope[Any]): Unit =
        if (projection.accepts(envelope.event)) handle(session, envelope.event)
    }

  private def groupedHandler[S <: JdbcSession](projection: EventProjection)(
    handle: (S, Seq[Any]) => Unit
  ): JdbcHandler[Seq[EventEnvelope[Any]], S] =
    new JdbcHandler[Seq[EventEnvelope[Any]], S] {
      override def process(session: S, envelopes: Seq[EventEnvelope[Any]]): Unit = {
        val events = BoundedHandlers.accepted(projection, envelopes)
        if (events.nonEmpty) handle(session, events)
      }
    }
}
