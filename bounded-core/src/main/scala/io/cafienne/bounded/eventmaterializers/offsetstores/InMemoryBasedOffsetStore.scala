/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.eventmaterializers.offsetstores

import org.apache.pekko.Done
import org.apache.pekko.persistence.query.Offset

import scala.collection.concurrent.TrieMap
import scala.concurrent.Future

/**
  * This Offset store keeps track of the offset only while the JVM is running. Does not store it's state to a persistent store.
  */
class InMemoryBasedOffsetStore extends OffsetStore {

  // Thread-safe: OffsetStoreProvider shares one instance between all materializers.
  val store: TrieMap[String, Offset] = TrieMap.empty

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
