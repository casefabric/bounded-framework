/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.projection

import org.apache.pekko.Done
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.persistence.query.Offset
import org.apache.pekko.projection.eventsourced.EventEnvelope
import org.apache.pekko.projection.scaladsl.{AtLeastOnceProjection, ExactlyOnceProjection, GroupedProjection}
import org.apache.pekko.projection.slick.{SlickHandler, SlickProjection}
import slick.basic.DatabaseConfig
import slick.dbio.DBIO
import slick.jdbc.JdbcProfile

import scala.concurrent.Future
import scala.reflect.ClassTag

/**
  * A Bounded read model as a `SlickProjection` (needs `pekko-projection-slick`), with its offsets in the database of
  * `databaseConfig`. Each method calls the `SlickProjection` method of the same name. It reads the events of the
  * aggregate type by tag, so it runs on the tag-based journals, not on R2DBC (use `BoundedR2dbcProjection` there).
  *
  * The handler function gets the events; events the filter of the `EventProjection` rejects skip it, and their offset
  * is stored. The projection publishes `EventProcessed`, and is then given to `adjust`, which can change it with the
  * methods of the Pekko projection. A `withStatusObserver` in `adjust` replaces the `EventProcessed` observer.
  */
object BoundedSlickProjection {
  private val store = "SlickProjection"

  /** The action of `handle` runs in the transaction that stores the offset. */
  def exactlyOnce[P <: JdbcProfile: ClassTag](
    system: ActorSystem[?],
    projection: EventProjection,
    databaseConfig: DatabaseConfig[P],
    adjust: ExactlyOnceProjection[Offset, EventEnvelope[Any]] => ExactlyOnceProjection[Offset, EventEnvelope[Any]] =
      identity
  )(handle: Any => DBIO[Done]): ExactlyOnceProjection[Offset, EventEnvelope[Any]] =
    adjust(
      SlickProjection
        .exactlyOnce(
          projection.projectionId,
          BoundedProjections.eventsByTag(system, projection, store),
          databaseConfig,
          () => handler(projection)(handle)
        )(implicitly[ClassTag[P]], system)
        .withStatusObserver(BoundedProjections.publishEventProcessed[EventEnvelope[Any]](system, projection))
    )

  /** The action of `handle` runs on its own; the offsets are stored after the events, so it must be idempotent. */
  def atLeastOnce[P <: JdbcProfile: ClassTag](
    system: ActorSystem[?],
    projection: EventProjection,
    databaseConfig: DatabaseConfig[P],
    adjust: AtLeastOnceProjection[Offset, EventEnvelope[Any]] => AtLeastOnceProjection[Offset, EventEnvelope[Any]] =
      identity
  )(handle: Any => DBIO[Done]): AtLeastOnceProjection[Offset, EventEnvelope[Any]] =
    adjust(
      SlickProjection
        .atLeastOnce(
          projection.projectionId,
          BoundedProjections.eventsByTag(system, projection, store),
          databaseConfig,
          () => handler(projection)(handle)
        )(implicitly[ClassTag[P]], system)
        .withStatusObserver(BoundedProjections.publishEventProcessed[EventEnvelope[Any]](system, projection))
    )

  /** The offsets are stored after the events, in batches; `handle` must be idempotent. */
  def atLeastOnceAsync[P <: JdbcProfile: ClassTag](
    system: ActorSystem[?],
    projection: EventProjection,
    databaseConfig: DatabaseConfig[P],
    adjust: AtLeastOnceProjection[Offset, EventEnvelope[Any]] => AtLeastOnceProjection[Offset, EventEnvelope[Any]] =
      identity
  )(handle: Any => Future[Done]): AtLeastOnceProjection[Offset, EventEnvelope[Any]] =
    adjust(
      SlickProjection
        .atLeastOnceAsync(
          projection.projectionId,
          BoundedProjections.eventsByTag(system, projection, store),
          databaseConfig,
          () => BoundedHandlers.async[EventEnvelope[Any]](projection)(handle)
        )(implicitly[ClassTag[P]], system)
        .withStatusObserver(BoundedProjections.publishEventProcessed[EventEnvelope[Any]](system, projection))
    )

  /**
    * `handle` gets the events in groups (size and time set with `withGroup`); its action runs in the transaction that
    * stores the offset of the group.
    */
  def groupedWithin[P <: JdbcProfile: ClassTag](
    system: ActorSystem[?],
    projection: EventProjection,
    databaseConfig: DatabaseConfig[P],
    adjust: GroupedProjection[Offset, EventEnvelope[Any]] => GroupedProjection[Offset, EventEnvelope[Any]] = identity
  )(handle: Seq[Any] => DBIO[Done]): GroupedProjection[Offset, EventEnvelope[Any]] =
    adjust(
      SlickProjection
        .groupedWithin(
          projection.projectionId,
          BoundedProjections.eventsByTag(system, projection, store),
          databaseConfig,
          () => groupedHandler(projection)(handle)
        )(implicitly[ClassTag[P]], system)
        .withStatusObserver(BoundedProjections.publishEventProcessed[EventEnvelope[Any]](system, projection))
    )

  /** `handle` gets the events in groups; the offset of a group is stored after it, so `handle` must be idempotent. */
  def groupedWithinAsync[P <: JdbcProfile: ClassTag](
    system: ActorSystem[?],
    projection: EventProjection,
    databaseConfig: DatabaseConfig[P],
    adjust: GroupedProjection[Offset, EventEnvelope[Any]] => GroupedProjection[Offset, EventEnvelope[Any]] = identity
  )(handle: Seq[Any] => Future[Done]): GroupedProjection[Offset, EventEnvelope[Any]] =
    adjust(
      SlickProjection
        .groupedWithinAsync(
          projection.projectionId,
          BoundedProjections.eventsByTag(system, projection, store),
          databaseConfig,
          () => BoundedHandlers.groupedAsync[EventEnvelope[Any]](projection)(handle)
        )(implicitly[ClassTag[P]], system)
        .withStatusObserver(BoundedProjections.publishEventProcessed[EventEnvelope[Any]](system, projection))
    )

  private def handler(projection: EventProjection)(handle: Any => DBIO[Done]): SlickHandler[EventEnvelope[Any]] =
    new SlickHandler[EventEnvelope[Any]] {
      override def process(envelope: EventEnvelope[Any]): DBIO[Done] =
        if (projection.accepts(envelope.event)) handle(envelope.event) else DBIO.successful(Done)
    }

  private def groupedHandler(projection: EventProjection)(
    handle: Seq[Any] => DBIO[Done]
  ): SlickHandler[Seq[EventEnvelope[Any]]] =
    new SlickHandler[Seq[EventEnvelope[Any]]] {
      override def process(envelopes: Seq[EventEnvelope[Any]]): DBIO[Done] = {
        val events = BoundedHandlers.accepted(projection, envelopes)
        if (events.nonEmpty) handle(events) else DBIO.successful(Done)
      }
    }
}
