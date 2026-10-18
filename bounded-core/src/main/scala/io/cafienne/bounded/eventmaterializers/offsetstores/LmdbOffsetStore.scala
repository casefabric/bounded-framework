/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.eventmaterializers.offsetstores

import java.io.File
import java.nio.{Buffer, ByteBuffer}
import java.nio.ByteBuffer.allocateDirect
import java.nio.charset.StandardCharsets.UTF_8
import org.apache.pekko.persistence.query.Offset
import com.typesafe.config.Config
import org.lmdbjava.{Dbi, DbiFlags, Env, EnvFlags}
import org.slf4j.LoggerFactory

import scala.concurrent.Future

private class LmdbOffsetStore(lmdbConfig: LmdbConfig) extends OffsetStore {

  val logger = LoggerFactory.getLogger(this.getClass)
  logger.info("Using LMDB offset store with configuration {}", lmdbConfig)
  // ONLY ONE ENVIRONMENT MUST BE OPEN FROM SAME PROCESS!!
  val env = Env.create
    .setMapSize(LmdbOffsetStore.DbSize)
    .setMaxDbs(1)
    .setMaxReaders(100)
    .open(lmdbConfig.path, EnvFlags.MDB_NOSUBDIR)
  val dbi: Dbi[ByteBuffer] = env.openDbi(LmdbOffsetStore.DbName, DbiFlags.MDB_CREATE)

  override def saveOffset(viewIdentifier: String, offset: Offset): Future[Unit] = {
    val txn = env.txnWrite()
    try {
      val keyByteBuffer = allocateDirect(100)
      keyByteBuffer.put(viewIdentifier.getBytes(UTF_8)).asInstanceOf[Buffer].flip

      val valueBytes      = OffsetCodec.encodeString(offset).getBytes(UTF_8)
      val valueByteBuffer = allocateDirect(valueBytes.length)
      valueByteBuffer.put(valueBytes).asInstanceOf[Buffer].flip()
      dbi.put(txn, keyByteBuffer, valueByteBuffer)
      txn.commit()
    } finally {
      txn.close()
    }
    Future.successful(())
  }

  override def getOffset(viewIdentifier: String): Future[Offset] = {
    val txn = env.txnRead()
    try {
      val keyByteBuffer = allocateDirect(100)
      keyByteBuffer.put(viewIdentifier.getBytes(UTF_8)).asInstanceOf[Buffer].flip
      val found = dbi.get(txn, keyByteBuffer)
      if (found != null) {
        val fetchedVal  = txn.`val`()
        val offsetValue = UTF_8.decode(fetchedVal).toString()
        val offset      = OffsetCodec.decodeString(offsetValue)
        Future.successful(offset)
      } else {
        Future.successful(Offset.noOffset)
      }
    } finally {
      txn.close()
    }
  }
}

class LmdbConfig(config: Config) {
  val path: File = new File(config.getString("path"))

  override def toString: String = s"LMDB config { path : ${path.getAbsolutePath} }"
}

object LmdbOffsetStore {

  // Upper bound of the memory map, not allocated up front. LMDB writes copy-on-write pages, so a
  // small map (it used to be 100 KB) fills up after a few saves.
  private val DbSize = 16L * 1024 * 1024
  private val DbName = "offsets"

  private var store: Option[LmdbOffsetStore] = None

  private val logger = LoggerFactory.getLogger("LmdbOffsetStore Singleton")

  def apply(lmdbConfig: LmdbConfig): OffsetStore = {
    if (store.isEmpty) {
      if (!lmdbConfig.path.exists()) {
        val parentFolder = new File(lmdbConfig.path.getParent)
        if (!parentFolder.exists()) {
          if (parentFolder.mkdirs()) {
            logger.debug("Created {} in order to store LMDB offsets", parentFolder.getAbsolutePath)
          } else {
            logger.debug(
              "Could not create {} in order to store LMDB offsets, please create this folder by hand and restart",
              parentFolder.getAbsolutePath
            )
          }
        }
      }
      store = Some(new LmdbOffsetStore(lmdbConfig))
    }
    store.get
  }

}
