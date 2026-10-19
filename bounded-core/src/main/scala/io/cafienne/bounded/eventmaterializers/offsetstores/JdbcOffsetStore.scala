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

  // The table is created if it does not exist, before the first operation.
  private val tableCreated = new CreateOnFirstUse(() => dbConfig.db.run(OffsetStore.schema.createIfNotExists))

  private def run[R](action: DBIO[R]): Future[R] = tableCreated().flatMap(_ => dbConfig.db.run(action))

  override def saveOffset(viewIdentifier: String, offset: PersistenceOffset): Future[Unit] = {
    val encoded = OffsetCodec.encode(offset)
    run(OffsetStore.insertOrUpdate((viewIdentifier, encoded.value, encoded.offsetType))).map(_ => ())
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
