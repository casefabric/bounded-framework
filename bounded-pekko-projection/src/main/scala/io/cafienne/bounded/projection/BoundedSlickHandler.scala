/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.projection

import org.apache.pekko.Done
import org.apache.pekko.persistence.query.typed.{EventEnvelope => SliceEnvelope}
import org.apache.pekko.projection.eventsourced.EventEnvelope
import org.apache.pekko.projection.slick.SlickHandler
import slick.dbio.DBIO

/**
  * The handler of a `SlickProjection` (needs `pekko-projection-slick`). With `SlickProjection.exactlyOnce`, the
  * action of `handle` runs in the transaction that stores the offset. Events the filter rejects skip `handle`; their
  * offset is stored.
  */
object BoundedSlickHandler {
  def apply[Envelope](projection: EventProjection)(handle: Any => DBIO[Done])(
    implicit envelope: ProjectionEnvelope[Envelope]
  ): SlickHandler[Envelope] = new SlickHandler[Envelope] {
    override def process(env: Envelope): DBIO[Done] = {
      val event = envelope.event(env)
      if (projection.accepts(event)) handle(event) else DBIO.successful(Done)
    }
  }

  /** The handler for the envelopes by tag, from `BoundedProjections.eventsByAggregateType`. */
  def byTag(projection: EventProjection)(handle: Any => DBIO[Done]): SlickHandler[EventEnvelope[Any]] =
    apply[EventEnvelope[Any]](projection)(handle)

  /** The handler for the envelopes by slices, from `BoundedProjections.eventsByAggregateTypeSlices`. */
  def bySlices(projection: EventProjection)(handle: Any => DBIO[Done]): SlickHandler[SliceEnvelope[Any]] =
    apply[SliceEnvelope[Any]](projection)(handle)
}
