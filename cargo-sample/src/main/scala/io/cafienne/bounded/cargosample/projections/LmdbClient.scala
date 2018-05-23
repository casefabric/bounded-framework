/*
 * Copyright (C) 2016-2018 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.cargosample.projections

import java.io.File

import org.lmdbjava.{Env, EnvFlags}
import org.lmdbjava.DbiFlags.MDB_CREATE
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.ByteBuffer.allocateDirect

trait LmdbClient {

  def put(key: String, value: String): Unit = ???

  def get(key: String): Option[String] = ???

  def delete(key: String): String = ???

}

class CargoLmdbClient(lmdbPath: File) extends LmdbClient {

  private val dbSize = 1000000

  val env = Env.create
    .setMapSize(dbSize)
    .setMaxDbs(1)
    .setMaxReaders(100)
    .open(lmdbPath, EnvFlags.MDB_NOSUBDIR)

  val dbi = env.openDbi(CargoLmdbClient.dbName, MDB_CREATE)

  override def put(key: String, value: String): Unit = {
    val txn = env.txnWrite()
    try {
      val keyByteBuffer = allocateDirect(100)
      keyByteBuffer.put(key.getBytes(UTF_8)).flip

      val byteArr         = value.getBytes(UTF_8)
      val valueByteBuffer = allocateDirect(byteArr.length)
      valueByteBuffer.put(byteArr).flip()
      dbi.put(txn, keyByteBuffer, valueByteBuffer)
      txn.commit()
    } finally {
      txn.close()
    }
  }

  override def get(key: String): Option[String] = {
    val txn = env.txnRead()
    try {
      val keyByteBuffer = allocateDirect(100)
      keyByteBuffer.put(key.getBytes(UTF_8)).flip

      val found = dbi.get(txn, keyByteBuffer)
      if (found != null) {
        val fetchedVal = txn.`val`()
        Some(UTF_8.decode(fetchedVal).toString())
      } else {
        None
      }
    } finally {
      txn.close()
    }
  }

  override def delete(key: String) = {
    throw new IllegalStateException("Not implemented")
  }

}

object CargoLmdbClient {
  val dbName = "cargo"
}
