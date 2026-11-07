/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.readside

/**
  * What a read side (a Pekko Projection run by `bounded-pekko-projection`, or an event materializer) publishes on
  * the event stream of its actor system when `bounded.eventmaterializers.publish` is on. `ReadSideRegistry` listens
  * to them; an application may too.
  *
  * The `readSideId` is stable: for a projection the id of its `EventProjection` (`name-aggregatetype`, the same for
  * all of its slice ranges), for an event materializer its `viewIdentifier`.
  */
sealed trait ReadSideSignal {
  def readSideId: String
}

/**
  * The read side has processed the event at `sequenceNr` of `persistenceId`: handled it, or skipped it because its
  * filter rejects it. For an exactly-once projection this is after the commit.
  */
final case class ProjectionProgress(readSideId: String, persistenceId: String, sequenceNr: Long) extends ReadSideSignal

/**
  * The read side failed to process the event at `sequenceNr` of `persistenceId`. It is `isFinal` when the read side
  * gives up on the event: a projection with the recovery strategy `fail` or `skip`, or after its last retry; an event
  * materializer whose restarts are used up. A failure that will be retried is published with `isFinal = false`.
  */
final case class ProjectionFailure(
  readSideId: String,
  persistenceId: String,
  sequenceNr: Long,
  cause: Throwable,
  isFinal: Boolean
) extends ReadSideSignal

/**
  * The read side's stream stopped with a failure. A projection may restart with backoff and catch up; an event
  * materializer publishes this once its restarts are used up.
  */
final case class ProjectionDown(readSideId: String, cause: Throwable) extends ReadSideSignal
