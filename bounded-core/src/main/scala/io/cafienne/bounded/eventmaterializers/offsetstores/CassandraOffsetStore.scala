/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.eventmaterializers.offsetstores

import org.apache.pekko.persistence.cassandra.query.scaladsl.CassandraReadJournal
import org.apache.pekko.persistence.query.Offset
import com.typesafe.config.Config
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

  /**
    * The keyspace of the offset store: the one set at `bounded.eventmaterializers.cassandra-offsetstore.keyspace`,
    * otherwise the keyspace of the configured journal plugin.
    *
    * @param journalPluginPath config path of the journal plugin, e.g. `pekko.persistence.cassandra.journal`
    */
  def keyspace(config: Config, journalPluginPath: String): String =
    if (config.hasPath(KeyspaceConfigPath)) config.getString(KeyspaceConfigPath)
    else config.getString(s"$journalPluginPath.keyspace")

  def createTableStatement(keyspace: String): String =
    s"CREATE TABLE IF NOT EXISTS $keyspace.vw_offsetstore (view_identifier text PRIMARY KEY, offset_type text, offset_value text);"

  def insertStatement(keyspace: String): String =
    s"""INSERT INTO $keyspace.vw_offsetstore ("view_identifier", "offset_type", "offset_value" ) VALUES (?, ?, ?)"""

  def selectStatement(keyspace: String): String =
    s"SELECT offset_value, offset_type FROM $keyspace.vw_offsetstore WHERE view_identifier = ?"
}
