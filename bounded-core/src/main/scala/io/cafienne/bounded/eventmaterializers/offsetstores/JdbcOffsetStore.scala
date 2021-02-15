/*
 * Copyright (C) 2016-2021 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.eventmaterializers.offsetstores

import akka.Done
import slick.basic.DatabaseConfig
import slick.jdbc.JdbcProfile
import slick.lifted.ProvenShape

import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import java.util.UUID
import java.util.concurrent.Executors

import akka.persistence.query.{
  NoOffset => PersistenceNoOffset,
  Offset => PersistenceOffset,
  Sequence => PersistenceSequence,
  TimeBasedUUID => PersistenceTimeBasedUUID
}

object OffsetStoreExecutionContext {

  private val concorrency                     = Runtime.getRuntime.availableProcessors()
  private val factor                          = 3 // get from configuration  file
  private val noOfThread                      = concorrency * factor
  implicit val ioThreadPool: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(noOfThread))

}
// Use this execution context for IO instead of scala execution context.

class JdbcOffsetStore(val dbConfig: DatabaseConfig[JdbcProfile]) extends OffsetStore {
  import dbConfig.profile.api._
  import OffsetStoreExecutionContext.ioThreadPool

  class OffsetTableRow(tag: Tag) extends Table[(String, String, String)](tag, "OFFSET") {
    def viewIdentifier: Rep[String]               = column[String]("VIEW_IDENTIFIER", O.PrimaryKey)
    def offset: Rep[String]                       = column[String]("OFFSET")
    def offsetType: Rep[String]                   = column[String]("OFFSET_TYPE")
    def * : ProvenShape[(String, String, String)] = (viewIdentifier, offset, offsetType)
  }
  lazy val OffsetStore = new TableQuery(tag => new OffsetTableRow(tag))

  //Create the table if it does not exist on startup.
  dbConfig.db.run(DBIO.seq(OffsetStore.schema.createIfNotExists))

  override def saveOffset(viewIdentifier: String, offset: PersistenceOffset): Future[Unit] = {
    val upsertQuery = offset match {
      case PersistenceNoOffset        => OffsetStore.insertOrUpdate((viewIdentifier, "0", "NoOffset"))
      case PersistenceSequence(value) => OffsetStore.insertOrUpdate((viewIdentifier, value.toString, "Sequence"))
      case PersistenceTimeBasedUUID(value) =>
        OffsetStore.insertOrUpdate((viewIdentifier, value.toString, "TimeBasedUUID"))
    }
    dbConfig.db.run(upsertQuery).map(_ => ())
  }

  override def getOffset(viewIdentifier: String): Future[PersistenceOffset] = {
    val query = OffsetStore.filter(_.viewIdentifier === viewIdentifier)
    dbConfig.db.run(query.result.headOption).map {
      case Some((_, offset, offsetType)) =>
        offsetType match {
          case "NoOffset"      => PersistenceNoOffset
          case "Sequence"      => PersistenceOffset.sequence(offset.toLong)
          case "TimeBasedUUID" => PersistenceOffset.timeBasedUUID(UUID.fromString(offset))
          case _               => PersistenceNoOffset
        }
      case None => PersistenceNoOffset
    }
  }

  override def clear(): Future[Done] = {
    val query = OffsetStore.delete
    dbConfig.db.run(query).map(_ => Done)
  }

  override def clear(viewIdentifier: String): Future[Done] = {
    val query = OffsetStore.filter(_.viewIdentifier === viewIdentifier).delete
    dbConfig.db.run(query).map(_ => Done)
  }
}
