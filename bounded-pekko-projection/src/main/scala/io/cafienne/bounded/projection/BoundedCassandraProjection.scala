/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.projection

import org.apache.pekko.Done
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.persistence.query.Offset
import org.apache.pekko.projection.cassandra.scaladsl.CassandraProjection
import org.apache.pekko.projection.eventsourced.EventEnvelope
import org.apache.pekko.projection.scaladsl.{AtLeastOnceProjection, GroupedProjection}

import scala.concurrent.Future

/**
  * A Bounded read model as a `CassandraProjection` (needs `pekko-projection-cassandra`), with its offsets in
  * Cassandra (`pekko.projection.cassandra`; create the table with `CassandraProjection.createTablesIfNotExists`).
  * Each method calls the `CassandraProjection` method of the same name. Cassandra has no transactions, so it is at
  * least once only: the offsets are stored after the events, and `handle` must be idempotent. It reads the events of
  * the aggregate type by tag, so it runs on the tag-based journals, not on R2DBC.
  *
  * The handler function gets the events; events the filter of the `EventProjection` rejects skip it, and their offset
  * is stored. The projection publishes `EventProcessed`, and is then given to `adjust`, which can change it with the
  * methods of the Pekko projection. A `withStatusObserver` in `adjust` replaces the `EventProcessed` observer.
  */
object BoundedCassandraProjection {
  private val store = "CassandraProjection"

  def atLeastOnce(
    system: ActorSystem[?],
    projection: EventProjection,
    adjust: AtLeastOnceProjection[Offset, EventEnvelope[Any]] => AtLeastOnceProjection[Offset, EventEnvelope[Any]] =
      identity
  )(handle: Any => Future[Done]): AtLeastOnceProjection[Offset, EventEnvelope[Any]] =
    adjust(
      CassandraProjection
        .atLeastOnce(
          projection.projectionId,
          BoundedProjections.eventsByTag(system, projection, store),
          () => BoundedHandlers.async[EventEnvelope[Any]](projection)(handle)
        )
        .withStatusObserver(BoundedProjections.publishEventProcessed[EventEnvelope[Any]](system, projection))
    )

  /** `handle` gets the events in groups (size and time set with `withGroup`). */
  def groupedWithin(
    system: ActorSystem[?],
    projection: EventProjection,
    adjust: GroupedProjection[Offset, EventEnvelope[Any]] => GroupedProjection[Offset, EventEnvelope[Any]] = identity
  )(handle: Seq[Any] => Future[Done]): GroupedProjection[Offset, EventEnvelope[Any]] =
    adjust(
      CassandraProjection
        .groupedWithin(
          projection.projectionId,
          BoundedProjections.eventsByTag(system, projection, store),
          () => BoundedHandlers.groupedAsync[EventEnvelope[Any]](projection)(handle)
        )
        .withStatusObserver(BoundedProjections.publishEventProcessed[EventEnvelope[Any]](system, projection))
    )
}
