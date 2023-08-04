/*
 * Copyright (C) 2016-2023 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.eventmaterializers

import io.cafienne.bounded.aggregate.DomainEvent

/**
  * The MaterializerEventFilter may be used to ensure that certain DomainEvents are *not* handled by the EventMaterializer
  */
trait MaterializerEventFilter {

  /**
    * function called in the Event Materializers for replay and live listening directly after the events are streamed.
    * @param evt DomainEvent that is to be evaluated
    * @return true when the event needs to be processed, false when it needs to be skipped
    */
  def filter(evt: DomainEvent): Boolean

}

/**
  * A Default Filter that will accept all events to be processed. This is used as default behaviour of the Event Materializers.
  */
object NoFilterEventFilter extends MaterializerEventFilter {
  override def filter(evt: DomainEvent): Boolean = true
}
