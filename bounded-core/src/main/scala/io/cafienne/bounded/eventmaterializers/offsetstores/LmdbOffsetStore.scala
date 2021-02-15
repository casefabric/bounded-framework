/*
 * Copyright (C) 2016-2021 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.eventmaterializers.offsetstores

import java.io.File
import java.nio.{Buffer, ByteBuffer}
import java.nio.ByteBuffer.allocateDirect
import java.nio.charset.StandardCharsets.UTF_8
import java.util.UUID

import akka.persistence.query.{Offset, Sequence, TimeBasedUUID}
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

      val valueByteBuffer = allocateDirect(300)
      valueByteBuffer.put(offset2String(offset).getBytes(UTF_8)).asInstanceOf[Buffer].flip()
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
        val offset      = string2offset(offsetValue)
        Future.successful(offset)
      } else {
        Future.successful(Offset.noOffset)
      }
    } finally {
      txn.close()
    }
  }

  private def offset2String(offset: Offset): String = offset match {
    case o: Sequence      => "sequence" + ":" + o.value.toString
    case o: TimeBasedUUID => "uuid" + ":" + o.value.toString
  }

  private def string2offset(offset: String): Offset = {
    val parts      = offset.split(":")
    val offsetType = parts(0)
    val offsetVal  = parts(1)
    offsetType match {
      case "uuid"     => Offset.timeBasedUUID(UUID.fromString(offsetVal))
      case "sequence" => Offset.sequence(offsetVal.toLong)
    }
  }

}

class LmdbConfig(config: Config) {
  val path: File = new File(config.getString("path"))

  override def toString: String = s"LMDB config { path : ${path.getAbsolutePath} }"
}

object LmdbOffsetStore {

  private val DbSize = 1024 * 100
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
