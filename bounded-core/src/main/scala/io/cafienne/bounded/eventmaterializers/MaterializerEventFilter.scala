/*
 * Copyright (C) 2016-2018 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.eventmaterializers

import io.cafienne.bounded._
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
  * This MaterializerEventFilter is used to ensure that events are only processed based on the compatibility rules given.
  * This allows to have event materializers that will only listen to in proc or current version messages (or a combination)
  * @see Compatibility
  * @param buildInfo Current build of the executing runtime
  * @param runtimeInfo Current indicator of the running system
  * @param compatible rules for compatibility to be used in the Event Materializer
  */
class RuntimeAndVersionMaterializerEventFilter(
  buildInfo: BuildInfo,
  runtimeInfo: RuntimeInfo,
  compatible: Compatibility = DefaultCompatibility
) extends MaterializerEventFilter {

  override def filter(evt: DomainEvent): Boolean = {
    compatible match {
      case Compatibility(RuntimeCompatibility.ALL, VersionCompatibility.ALL)       => true
      case Compatibility(RuntimeCompatibility.ALL, VersionCompatibility.TILL_DATE) => true
      case Compatibility(RuntimeCompatibility.ALL, VersionCompatibility.CURRENT)   => true
      case Compatibility(RuntimeCompatibility.CURRENT, VersionCompatibility.ALL) =>
        evt.metaData.runTimeInfo.id.equals(runtimeInfo.id)
      case Compatibility(RuntimeCompatibility.CURRENT, VersionCompatibility.TILL_DATE) => true
      case Compatibility(RuntimeCompatibility.CURRENT, VersionCompatibility.CURRENT)   => true
    }
  }
}

/**
  * A Default Filter that will accept all events to be processed. This is used as default behaviour of the Event Materializers.
  */
object NoFilterEventFilter extends MaterializerEventFilter {
  override def filter(evt: DomainEvent): Boolean = true
}
