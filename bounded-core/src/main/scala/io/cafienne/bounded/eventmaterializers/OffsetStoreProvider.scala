/*
 * Copyright (C) 2016-2018 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.eventmaterializers

import akka.Done
import akka.persistence.query.Offset
import com.typesafe.config.Config
import io.cafienne.bounded.akka.ActorSystemProvider
import io.cafienne.bounded.eventmaterializers.offsetstores.{
  InMemoryBasedOffsetStore,
  LmdbConfig,
  LmdbOffsetStore,
  OffsetStore
}

import scala.collection.mutable
import scala.concurrent.Future

trait OffsetStoreProvider extends OffsetStore {
  this: ActorSystemProvider =>

  private val offsetStoreConfig = system.settings.config.getConfig("bounded.eventmaterializers.offsetstore")

  private val store: OffsetStore = OffsetStoreProvider.getStore(offsetStoreConfig)

  override def saveOffset(viewIdentifier: String, offset: Offset): Future[Unit] =
    store.saveOffset(viewIdentifier, offset)

  override def getOffset(viewIdentifier: String): Future[Offset] = store.getOffset(viewIdentifier)

  override def clear(): Future[Done] = store.clear()

  override def clear(viewIdentifier: String): Future[Done] = store.clear(viewIdentifier)

}

object OffsetStoreProvider {

  val ImMemoryStoreName = "inmemory"
  val LmdbStoreName     = "lmdb"

  private val store = mutable.Map.empty[String, OffsetStore]

  def getStore(offsetStoreConfig: Config): OffsetStore = {
    offsetStoreConfig.getString("type") match {
      case LmdbStoreName     => getLmdbStore(offsetStoreConfig)
      case ImMemoryStoreName => getInMemoryStore()
      case other             => throw new RuntimeException(s"Offsetstore $other is not supported by OffsetStoreProvider")
    }
  }

  def getInMemoryStore(): OffsetStore = {
    store.getOrElse(ImMemoryStoreName, {
      val inmemOffsetStore = new InMemoryBasedOffsetStore()
      store.put(ImMemoryStoreName, inmemOffsetStore)
      inmemOffsetStore
    })
  }

  def getLmdbStore(offsetStoreConfig: Config): OffsetStore = {
    store.getOrElse(
      LmdbStoreName, {
        val lmdbConfig      = new LmdbConfig(offsetStoreConfig)
        val lmdbOffsetStore = LmdbOffsetStore(lmdbConfig)
        store.put(LmdbStoreName, lmdbOffsetStore)
        lmdbOffsetStore
      }
    )
  }

}
