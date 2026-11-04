/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.projection

import org.apache.pekko.persistence.query.Offset
import org.apache.pekko.persistence.query.typed.{EventEnvelope => SliceEnvelope}
import org.apache.pekko.projection.eventsourced.EventEnvelope

/**
  * What the handlers read from an envelope. There are instances for the envelopes of both source providers: by tag
  * (`EventEnvelope`) and by slices on the R2DBC journal (`persistence.query.typed.EventEnvelope`).
  */
trait ProjectionEnvelope[Envelope] {
  def event(envelope: Envelope): Any
  def offset(envelope: Envelope): Offset
  def persistenceId(envelope: Envelope): String
  def sequenceNr(envelope: Envelope): Long
}

object ProjectionEnvelope {

  implicit def byTag[Event]: ProjectionEnvelope[EventEnvelope[Event]] = new ProjectionEnvelope[EventEnvelope[Event]] {
    def event(envelope: EventEnvelope[Event]): Any            = envelope.event
    def offset(envelope: EventEnvelope[Event]): Offset        = envelope.offset
    def persistenceId(envelope: EventEnvelope[Event]): String = envelope.persistenceId
    def sequenceNr(envelope: EventEnvelope[Event]): Long      = envelope.sequenceNr
  }

  implicit def bySlices[Event]: ProjectionEnvelope[SliceEnvelope[Event]] =
    new ProjectionEnvelope[SliceEnvelope[Event]] {
      def event(envelope: SliceEnvelope[Event]): Any            = envelope.event
      def offset(envelope: SliceEnvelope[Event]): Offset        = envelope.offset
      def persistenceId(envelope: SliceEnvelope[Event]): String = envelope.persistenceId
      def sequenceNr(envelope: SliceEnvelope[Event]): Long      = envelope.sequenceNr
    }
}
