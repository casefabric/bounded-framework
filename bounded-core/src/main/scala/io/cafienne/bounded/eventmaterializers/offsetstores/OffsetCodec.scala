/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.eventmaterializers.offsetstores

import java.net.{URLDecoder, URLEncoder}
import java.nio.charset.StandardCharsets.UTF_8
import java.time.Instant
import java.util.UUID

import org.apache.pekko.persistence.query.{NoOffset, Offset, Sequence, TimeBasedUUID, TimestampOffset}
import io.cafienne.bounded.eventmaterializers.TagOffsets
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

/**
  * An offset as the offset stores keep it: a type name and a value.
  */
final case class EncodedOffset(offsetType: String, value: String)

/**
  * Turns offsets into a type name and value and back, the same for every offset store.
  *
  * Reading never fails: a blank, unknown or unreadable record is read as NoOffset with a warning, so the
  * materializer replays from the start (handlers are at-least-once) instead of failing at startup.
  */
object OffsetCodec {

  private val logger = LoggerFactory.getLogger(getClass)

  val NoOffsetType  = "none"
  val SequenceType  = "sequence"
  val UuidType      = "uuid"
  val TimestampType = "timestamp"
  val TagsType      = "tags"

  def encode(offset: Offset): EncodedOffset = offset match {
    case NoOffset         => EncodedOffset(NoOffsetType, "")
    case Sequence(value)  => EncodedOffset(SequenceType, value.toString)
    case TimeBasedUUID(v) => EncodedOffset(UuidType, v.toString)
    case t: TimestampOffset =>
      EncodedOffset(TimestampType, (t.timestamp.toString +: t.seen.toSeq.map(pair)).mkString(";"))
    case TagOffsets(offsets) =>
      EncodedOffset(TagsType, offsets.map { case (tag, o) => escape(tag) + "=" + escape(encodeString(o)) }.mkString("&"))
    case other =>
      throw new IllegalArgumentException(s"Offset type ${other.getClass.getName} cannot be stored")
  }

  def decode(offsetType: String, value: String): Offset =
    Try(decodeOrFail(offsetType, value)) match {
      case Success(offset) => offset
      case Failure(e) =>
        logger.warn(s"Cannot read stored offset of type [$offsetType] with value [$value], using NoOffset: ${e.getMessage}")
        NoOffset
    }

  /**
    * The offset as one string, `type:value`, for stores that keep a single value.
    */
  def encodeString(offset: Offset): String = {
    val encoded = encode(offset)
    s"${encoded.offsetType}:${encoded.value}"
  }

  def decodeString(stored: String): Offset = stored.split(":", 2) match {
    case Array(offsetType, value) => decode(offsetType, value)
    case _                        => decode(stored, "")
  }

  // Type names are matched case-insensitively and include the names JdbcOffsetStore used to write.
  private def decodeOrFail(offsetType: String, value: String): Offset =
    Option(offsetType).map(_.trim.toLowerCase).getOrElse("") match {
      case NoOffsetType | "nooffset"  => NoOffset
      case SequenceType               => Offset.sequence(value.trim.toLong)
      case UuidType | "timebaseduuid" => Offset.timeBasedUUID(UUID.fromString(value.trim))
      case TimestampType              => decodeTimestamp(value)
      case TagsType                   => decodeTags(value)
      case other                      => throw new IllegalArgumentException(s"unknown offset type [$other]")
    }

  private def decodeTimestamp(value: String): TimestampOffset = {
    val parts = value.split(";").toList
    val seen = parts.tail.map { entry =>
      entry.split("=", 2) match {
        case Array(pid, seqNr) => unescape(pid) -> seqNr.toLong
        case _                 => throw new IllegalArgumentException(s"invalid seen entry [$entry]")
      }
    }
    TimestampOffset(Instant.parse(parts.head), seen.toMap)
  }

  private def decodeTags(value: String): TagOffsets =
    TagOffsets(
      value
        .split("&")
        .filter(_.nonEmpty)
        .map { entry =>
          entry.split("=", 2) match {
            case Array(tag, offset) => unescape(tag) -> decodeStringOrFail(unescape(offset))
            case _                  => throw new IllegalArgumentException(s"invalid tag entry [$entry]")
          }
        }
        .toMap
    )

  // Inside TagOffsets one unreadable tag offset makes the whole record unreadable, rather than replaying only that tag.
  private def decodeStringOrFail(stored: String): Offset = stored.split(":", 2) match {
    case Array(offsetType, value) => decodeOrFail(offsetType, value)
    case _                        => decodeOrFail(stored, "")
  }

  private def pair(seen: (String, Long)): String = escape(seen._1) + "=" + seen._2

  private def escape(s: String): String = URLEncoder.encode(s, UTF_8.name())

  private def unescape(s: String): String = URLDecoder.decode(s, UTF_8.name())
}
