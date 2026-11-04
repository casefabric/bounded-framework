/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.projection

import org.apache.pekko.Done
import org.apache.pekko.persistence.query.typed.{EventEnvelope => SliceEnvelope}
import org.apache.pekko.projection.eventsourced.EventEnvelope
import org.apache.pekko.projection.r2dbc.scaladsl.{R2dbcHandler, R2dbcSession}

import scala.concurrent.Future

/**
  * The handler of an `R2dbcProjection` (needs `pekko-projection-r2dbc`), usually on the envelopes of
  * `BoundedProjections.eventsByAggregateTypeSlices`. With `R2dbcProjection.exactlyOnce`, the statements `handle` runs
  * on the session are in the transaction that stores the offset. Events the filter rejects skip `handle`; their
  * offset is stored.
  */
object BoundedR2dbcHandler {
  def apply[Envelope](projection: EventProjection)(handle: (R2dbcSession, Any) => Future[Done])(
    implicit envelope: ProjectionEnvelope[Envelope]
  ): R2dbcHandler[Envelope] = new R2dbcHandler[Envelope] {
    override def process(session: R2dbcSession, env: Envelope): Future[Done] = {
      val event = envelope.event(env)
      if (projection.accepts(event)) handle(session, event) else Future.successful(Done)
    }
  }

  /** The handler for the envelopes by slices, from `BoundedProjections.eventsByAggregateTypeSlices`. */
  def bySlices(projection: EventProjection)(
    handle: (R2dbcSession, Any) => Future[Done]
  ): R2dbcHandler[SliceEnvelope[Any]] =
    apply[SliceEnvelope[Any]](projection)(handle)

  /** The handler for the envelopes by tag, from `BoundedProjections.eventsByAggregateType`. */
  def byTag(projection: EventProjection)(
    handle: (R2dbcSession, Any) => Future[Done]
  ): R2dbcHandler[EventEnvelope[Any]] =
    apply[EventEnvelope[Any]](projection)(handle)
}
