/*
 * Copyright (C) 2016-2018 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.akka.persistence.eventmaterializers

import java.util.UUID

import akka.persistence.cassandra.query.scaladsl.CassandraReadJournal
import akka.persistence.query.{Offset, Sequence, TimeBasedUUID}
import io.cafienne.bounded.akka.persistence.ReadJournalProvider

import scala.collection.mutable.Map
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

final case class EventNumber(value: Int) extends Offset

trait OffsetStore {

  def saveOffset(viewIdentifier: String, offset: Offset): Future[Unit] = ???

  def getOffset(viewIdentifier: String): Future[Offset] = ???

  /**
    * The start offset is the offset to be used to start listening to the start of a stream.
    * The offset type may differ per store implementation. At this moment only the timeBasedUUID offset is supported
    * For cassandra, the first-time-bucket configuration value is used to create this start offset.
    * @return
    */
  def startOffset: Offset = ???

}

object OffsetTypes extends Enumeration {
  type OffsetTypes = Value
  val UuidType, SequenceType, EventNumberType = Value
}
import OffsetTypes._

/**
  * This Offset store keeps track of the offset only while the JVM is running. Does not store it's state to a persistent store.
  */
class InMemoryBasedOffsetStore(offsetType: OffsetTypes) extends OffsetStore {
  val store: Map[String, Offset] = Map.empty

  override def saveOffset(viewIdentifier: String, offset: Offset): Future[Unit] = {
    store += ((viewIdentifier, offset))
    Future.successful({})
  }

  override def getOffset(viewIdentifier: String): Future[Offset] = {
    Future.successful(store.getOrElse(viewIdentifier, startOffset))
  }

  override def startOffset: Offset = {
    if (offsetType.equals(EventNumberType)) EventNumber(0)
    else Offset.sequence(0L)
  }
}

class CassandraOffsetStore(readJournal: CassandraReadJournal, offsetType: OffsetTypes) extends OffsetStore {
  import EventMaterializerExecutionContext._

  Await.result(
    readJournal.session.executeWrite(
      "CREATE TABLE IF NOT EXISTS akka.vw_offsetstore (view_identifier text PRIMARY KEY, offset_type text, offset_value text);"
    ),
    3.seconds
  )

  override def saveOffset(viewIdentifier: String, offset: Offset): Future[Unit] = {
    val sov = if (offset == Offset.noOffset) startOffset else offset

    readJournal.session
      .executeWrite(
        "INSERT INTO akka.vw_offsetstore (\"view_identifier\", \"offset_type\", \"offset_value\" ) VALUES (?, ?, ?)",
        viewIdentifier,
        offsetType2String(offsetType),
        offset2String(sov)
      )
      .map(s => {})
  }

  override def getOffset(viewIdentifier: String): Future[Offset] = {
    readJournal.session
      .selectOne(s"SELECT offset_value, offset_type FROM akka.vw_offsetstore WHERE view_identifier=\'$viewIdentifier\'")
      .map(r => r.fold(startOffset)(r => string2Offset(r.getString("offset_value"), r.getString("offset_type"))))
  }

  override def startOffset: Offset = offsetType match {
    case EventNumberType => Offset.noOffset
    case SequenceType    => Offset.sequence(0L)
    case _ =>
      Offset.timeBasedUUID(readJournal.asInstanceOf[CassandraReadJournal].firstOffset)
  }

  private def offset2String(offset: Offset): String = offsetType match {
    case EventNumberType => offset.asInstanceOf[EventNumber].value.toString
    case SequenceType    => offset.asInstanceOf[Sequence].value.toString
    case _               => offset.asInstanceOf[TimeBasedUUID].value.toString
  }

  private def string2Offset(offsetVal: String, offsetType: String): Offset =
    offsetType match {
      case "uuid"        => Offset.timeBasedUUID(UUID.fromString(offsetVal))
      case "eventnumber" => EventNumber(offsetVal.toInt)
      case "sequence"    => Offset.sequence(offsetVal.toLong)
      case _             => startOffset
    }

  private def offsetType2String(ot: OffsetTypes) = ot match {
    case EventNumberType => "eventnumber"
    case SequenceType    => "sequence"
    case _               => "uuid"
  }

}

protected trait OffsetType {
  def offsetType: OffsetTypes = ???
}

trait OffsetTypeUuid extends OffsetType {
  override def offsetType: OffsetTypes = UuidType
}

trait OffsetTypeSequence extends OffsetType {
  override def offsetType: OffsetTypes = SequenceType
}

trait OffsetTypeEventNumber extends OffsetType {
  override def offsetType: OffsetTypes = EventNumberType
}

trait ReadJournalOffsetStore extends OffsetStore with OffsetType {
  readJournalProvider: ReadJournalProvider =>

  val store: OffsetStore = {
    if (configuredJournal.endsWith("cassandra-journal")) {
      new CassandraOffsetStore(readJournal.asInstanceOf[CassandraReadJournal], offsetType)
    } else if (configuredJournal.endsWith("inmemory-journal")) {
      new InMemoryBasedOffsetStore(offsetType)
    } else if (configuredJournal.endsWith("leveldb")) {
      //TODO write a specific leveldb offset store implementation (HIGH priority)
      new InMemoryBasedOffsetStore(offsetType)
    } else {
      throw new RuntimeException(s"Offsetstore $configuredJournal is not supported as ReadJournalOffsetStore")
    }
  }

  override def saveOffset(viewIdentifier: String, offset: Offset): Future[Unit] =
    store.saveOffset(viewIdentifier, offset)

  override def getOffset(viewIdentifier: String): Future[Offset] =
    store.getOffset(viewIdentifier)

  override def startOffset: Offset = store.startOffset

}
