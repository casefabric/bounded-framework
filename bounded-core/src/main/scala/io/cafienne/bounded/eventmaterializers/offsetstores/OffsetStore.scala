/*
 * Copyright (C) 2016-2020 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.eventmaterializers.offsetstores

import akka.Done
import akka.persistence.query.Offset

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
