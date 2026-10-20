/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.eventmaterializers.offsetstores

import org.apache.pekko.persistence.cassandra.query.scaladsl.CassandraReadJournal
import org.apache.pekko.persistence.query.Offset
import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import io.cafienne.bounded.eventmaterializers.{EventMaterializerExecutionContext}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class CassandraOffsetStore(readJournal: CassandraReadJournal, createTableTimeout: Duration, keyspace: String = "akka")
    extends OffsetStore {

  import EventMaterializerExecutionContext._
  import CassandraOffsetStore._

  Await.result(readJournal.session.executeWrite(createTableStatement(keyspace)), createTableTimeout)

  override def saveOffset(viewIdentifier: String, offset: Offset): Future[Unit] = {
    readJournal.session
      .executeWrite(
        insertStatement(keyspace),
        viewIdentifier,
        offset2type(offset),
        offset2String(offset)
      )
      .map(_ => {})
  }

  override def getOffset(viewIdentifier: String): Future[Offset] = {
    readJournal.session
      .selectOne(selectStatement(keyspace), viewIdentifier)
      .map(r => r.fold(Offset.noOffset)(r => string2offset(r.getString("offset_value"), r.getString("offset_type"))))
  }

  protected def offset2String(offset: Offset): String = OffsetCodec.encode(offset).value

  protected def string2offset(offsetVal: String, offsetType: String): Offset = OffsetCodec.decode(offsetType, offsetVal)

  protected def offset2type(ot: Offset): String = OffsetCodec.encode(ot).offsetType
}

object CassandraOffsetStore {

  val KeyspaceConfigPath = "bounded.eventmaterializers.cassandra-offsetstore.keyspace"

  // Older versions read the keyspace from these keys and failed to start without one, so a deployment that
  // stored offsets has one of them set.
  val LegacyKeyspaceConfigPaths = Seq("cassandra.journal.keyspace", "cassandra-journal.keyspace")

  private val logger = LoggerFactory.getLogger(getClass)

  /**
    * The keyspace of the offset store: the one set at `bounded.eventmaterializers.cassandra-offsetstore.keyspace`,
    * else the one at an old key (`cassandra.journal.keyspace` or `cassandra-journal.keyspace`), so that an upgraded
    * deployment keeps reading its stored offsets, else the keyspace of the configured journal plugin.
    *
    * @param journalPluginPath config path of the journal plugin, e.g. `pekko.persistence.cassandra.journal`
    */
  def keyspace(config: Config, journalPluginPath: String): String =
    if (config.hasPath(KeyspaceConfigPath)) config.getString(KeyspaceConfigPath)
    else
      LegacyKeyspaceConfigPaths.find(config.hasPath) match {
        case Some(legacyPath) =>
          val keyspace = config.getString(legacyPath)
          logger.warn(s"Offset store keyspace $keyspace is read from the old key $legacyPath, set $KeyspaceConfigPath instead")
          keyspace
        case None => config.getString(s"$journalPluginPath.keyspace")
      }

  def createTableStatement(keyspace: String): String =
    s"CREATE TABLE IF NOT EXISTS $keyspace.vw_offsetstore (view_identifier text PRIMARY KEY, offset_type text, offset_value text);"

  def insertStatement(keyspace: String): String =
    s"""INSERT INTO $keyspace.vw_offsetstore ("view_identifier", "offset_type", "offset_value" ) VALUES (?, ?, ?)"""

  def selectStatement(keyspace: String): String =
    s"SELECT offset_value, offset_type FROM $keyspace.vw_offsetstore WHERE view_identifier = ?"
}
