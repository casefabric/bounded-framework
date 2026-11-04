/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.projection

import org.apache.pekko.persistence.query.typed.{EventEnvelope => SliceEnvelope}
import org.apache.pekko.projection.eventsourced.EventEnvelope
import org.apache.pekko.projection.jdbc.JdbcSession
import org.apache.pekko.projection.jdbc.scaladsl.JdbcHandler

/**
  * The handler of a `JdbcProjection` (needs `pekko-projection-jdbc`). With `JdbcProjection.exactlyOnce`, `handle`
  * runs in the transaction that stores the offset, so its writes and the offset are committed together or not at
  * all. Events the filter rejects skip `handle`; their offset is stored.
  */
object BoundedJdbcHandler {
  def apply[Envelope, S <: JdbcSession](projection: EventProjection)(handle: (S, Any) => Unit)(
    implicit envelope: ProjectionEnvelope[Envelope]
  ): JdbcHandler[Envelope, S] = new JdbcHandler[Envelope, S] {
    override def process(session: S, env: Envelope): Unit = {
      val event = envelope.event(env)
      if (projection.accepts(event)) handle(session, event)
    }
  }

  /** The handler for the envelopes by tag, from `BoundedProjections.eventsByAggregateType`. */
  def byTag[S <: JdbcSession](projection: EventProjection)(
    handle: (S, Any) => Unit
  ): JdbcHandler[EventEnvelope[Any], S] =
    apply[EventEnvelope[Any], S](projection)(handle)

  /** The handler for the envelopes by slices, from `BoundedProjections.eventsByAggregateTypeSlices`. */
  def bySlices[S <: JdbcSession](projection: EventProjection)(
    handle: (S, Any) => Unit
  ): JdbcHandler[SliceEnvelope[Any], S] =
    apply[SliceEnvelope[Any], S](projection)(handle)
}
