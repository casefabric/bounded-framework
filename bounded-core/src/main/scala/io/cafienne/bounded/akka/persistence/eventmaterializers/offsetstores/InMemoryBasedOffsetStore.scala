/*
 * Copyright (C) 2016-2018 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.akka.persistence.eventmaterializers.offsetstores

import akka.Done
import akka.persistence.query.Offset

import scala.concurrent.Future

/**
  * This Offset store keeps track of the offset only while the JVM is running. Does not store it's state to a persistent store.
  */
class InMemoryBasedOffsetStore extends OffsetStore {

  import scala.collection.mutable.Map

  val store: Map[String, Offset] = Map.empty

  override def saveOffset(viewIdentifier: String, offset: Offset): Future[Unit] = {
    store += ((viewIdentifier, offset))
    Future.successful({})
  }

  override def getOffset(viewIdentifier: String): Future[Offset] = {
    Future.successful(store.getOrElse(viewIdentifier, Offset.noOffset))
  }

  override def clear(): Future[Done] = {
    store.clear()
    Future.successful(Done)
  }

  override def clear(viewIdentifier: String): Future[Done] = {
    store.remove(viewIdentifier)
    Future.successful(Done)
  }

}
