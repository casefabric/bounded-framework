/*
 * Copyright (C) 2016-2018 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.akka.persistence.eventmaterializers.offsetstores

import java.util.UUID

import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.query.{Offset, Sequence, TimeBasedUUID}
import io.cafienne.bounded.akka.persistence.eventmaterializers.{EventMaterializerExecutionContext}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class CassandraOffsetStore(readJournal: CassandraReadJournal, createTableTimeout: Duration, keyspace: String = "akka")
    extends OffsetStore {

  import EventMaterializerExecutionContext._

  Await.result(
    readJournal.session.executeWrite(
      s"CREATE TABLE IF NOT EXISTS $keyspace.vw_offsetstore (view_identifier text PRIMARY KEY, offset_type text, offset_value text);"
    ),
    createTableTimeout
  )

  override def saveOffset(viewIdentifier: String, offset: Offset): Future[Unit] = {
    readJournal.session
      .executeWrite(
        "INSERT INTO akka.vw_offsetstore (\"view_identifier\", \"offset_type\", \"offset_value\" ) VALUES (?, ?, ?)",
        viewIdentifier,
        offset2type(offset),
        offset2String(offset)
      )
      .map(s => {})
  }

  override def getOffset(viewIdentifier: String): Future[Offset] = {
    readJournal.session
      .selectOne(
        s"SELECT offset_value, offset_type FROM $keyspace.vw_offsetstore WHERE view_identifier=\'$viewIdentifier\'"
      )
      .map(r => r.fold(Offset.noOffset)(r => string2offset(r.getString("offset_value"), r.getString("offset_type"))))
  }

  protected def offset2String(offset: Offset): String = offset match {
    case o: Sequence      => o.value.toString
    case o: TimeBasedUUID => o.value.toString
  }

  protected def string2offset(offsetVal: String, offsetType: String): Offset =
    offsetType match {
      case "uuid"     => Offset.timeBasedUUID(UUID.fromString(offsetVal))
      case "sequence" => Offset.sequence(offsetVal.toLong)
    }

  protected def offset2type(ot: Offset) = ot match {
    case _: Sequence      => "sequence"
    case _: TimeBasedUUID => "uuid"
  }
}
