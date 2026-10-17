/*
 * Copyright (C) 2016-2025 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.eventmaterializers.offsetstores

import org.apache.pekko.Done
import org.apache.pekko.persistence.query.Offset

import scala.concurrent.Future

final case class EventNumber(value: Int) //extends Offset

trait OffsetStore {

  def saveOffset(viewIdentifier: String, offset: Offset): Future[Unit] = ???

  def getOffset(viewIdentifier: String): Future[Offset] = ???

  def clear(): Future[Done] = {
    throw new IllegalStateException("Operation clear is not implemented for this OffsetStore")
  }

  def clear(viewIdentifier: String): Future[Done] = {
    throw new IllegalStateException("Operation clear is not implemented for this OffsetStore")
  }

}
