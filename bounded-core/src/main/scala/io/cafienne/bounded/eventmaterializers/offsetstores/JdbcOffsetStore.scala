/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.eventmaterializers.offsetstores

import org.apache.pekko.Done
import com.typesafe.config.Config
import slick.basic.DatabaseConfig
import slick.jdbc.JdbcProfile
import slick.lifted.ProvenShape

import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import java.util.concurrent.Executors

import org.apache.pekko.persistence.query.{NoOffset => PersistenceNoOffset, Offset => PersistenceOffset}

object OffsetStoreExecutionContext {

  private val concorrency                     = Runtime.getRuntime.availableProcessors()
  private val factor                          = 3 // get from configuration  file
  private val noOfThread                      = concorrency * factor
  implicit val ioThreadPool: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(noOfThread))

}
// Use this execution context for IO instead of scala execution context.

/**
  * Offset store in the table `OFFSET`. Its `OFFSET` column is created as VARCHAR(4000), so the offsets of a saga
  * over many tags fit. A table created by an older version keeps the column it has: on databases that give an
  * unsized string column a bounded type (Oracle, SQL Server and Derby make it VARCHAR(254)), widen it by hand
  * before running sagas with many or long tags. An offset that does not fit fails its save.
  */
class JdbcOffsetStore(val dbConfig: DatabaseConfig[JdbcProfile]) extends OffsetStore {
  import dbConfig.profile.api._
  import OffsetStoreExecutionContext.ioThreadPool
  import JdbcOffsetStore._

  class OffsetTableRow(tag: Tag) extends Table[(String, String, String)](tag, "OFFSET") {
    def viewIdentifier: Rep[String]               = column[String]("VIEW_IDENTIFIER", O.PrimaryKey)
    def offset: Rep[String]                       = column[String]("OFFSET", O.Length(MaxOffsetLength))
    def offsetType: Rep[String]                   = column[String]("OFFSET_TYPE")
    def * : ProvenShape[(String, String, String)] = (viewIdentifier, offset, offsetType)
  }
  lazy val OffsetStore = new TableQuery(tag => new OffsetTableRow(tag))

  // The table is created if it does not exist, before the first operation.
  private val tableCreated = new CreateOnFirstUse(() => dbConfig.db.run(OffsetStore.schema.createIfNotExists))

  private def run[R](action: DBIO[R]): Future[R] = tableCreated().flatMap(_ => dbConfig.db.run(action))

  override def saveOffset(viewIdentifier: String, offset: PersistenceOffset): Future[Unit] = {
    val encoded = OffsetCodec.encode(offset)
    if (encoded.value.length > MaxOffsetLength) {
      Future.failed(
        new IllegalArgumentException(
          s"Offset of $viewIdentifier is ${encoded.value.length} characters, the OFFSET column holds $MaxOffsetLength"
        )
      )
    } else {
      val offsetType = LegacyTypeNames.getOrElse(encoded.offsetType, encoded.offsetType)
      run(OffsetStore.insertOrUpdate((viewIdentifier, encoded.value, offsetType))).map(_ => ())
    }
  }

  override def getOffset(viewIdentifier: String): Future[PersistenceOffset] = {
    val query = OffsetStore.filter(_.viewIdentifier === viewIdentifier)
    run(query.result.headOption).map {
      case Some((_, offset, offsetType)) => OffsetCodec.decode(offsetType, offset)
      case None                          => PersistenceNoOffset
    }
  }

  override def clear(): Future[Done] = run(OffsetStore.delete).map(_ => Done)

  override def clear(viewIdentifier: String): Future[Done] =
    run(OffsetStore.filter(_.viewIdentifier === viewIdentifier).delete).map(_ => Done)
}

object JdbcOffsetStore {

  val MaxOffsetLength = 4000

  // The type names older versions wrote and read, kept so that they can read the offsets this version writes.
  private val LegacyTypeNames = Map(
    OffsetCodec.NoOffsetType -> "NoOffset",
    OffsetCodec.SequenceType -> "Sequence",
    OffsetCodec.UuidType     -> "TimeBasedUUID"
  )

  val DatabaseConfigPath       = "bounded.eventmaterializers.jdbc-offsetstore.database-config"
  val LegacyDatabaseConfigPath = "pekko.persistence.offset.r2dbc.store"

  /**
    * The config path of the Slick database config for the offset store: the one named at
    * `bounded.eventmaterializers.jdbc-offsetstore.database-config`, else at the old key
    * `pekko.persistence.offset.r2dbc.store`, else the `slick` config of the journal plugin.
    *
    * @param journalPluginPath config path of the journal plugin, e.g. `jdbc-journal`
    */
  def databaseConfigPath(config: Config, journalPluginPath: String): String =
    if (config.hasPath(DatabaseConfigPath)) config.getString(DatabaseConfigPath)
    else if (config.hasPath(LegacyDatabaseConfigPath)) config.getString(LegacyDatabaseConfigPath)
    else s"$journalPluginPath.slick"
}
