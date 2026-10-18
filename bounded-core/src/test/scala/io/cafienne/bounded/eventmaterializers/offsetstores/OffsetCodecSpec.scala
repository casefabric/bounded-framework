/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.eventmaterializers.offsetstores

import java.time.Instant
import java.util.UUID

import org.apache.pekko.persistence.query.{NoOffset, Offset, TimestampOffset}
import io.cafienne.bounded.eventmaterializers.TagOffsets
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

object OffsetCodecSpec {
  val timeBasedUuid: UUID = UUID.fromString("49225740-2019-11ea-a752-ffae2393b6e4")

  val timestampOffset: TimestampOffset =
    TimestampOffset(Instant.parse("2026-10-18T12:34:56.123456Z"), Map("ar-1" -> 3L, "odd;=&%:id" -> 7L))

  val allOffsetTypes: Seq[Offset] = Seq(
    NoOffset,
    Offset.sequence(42L),
    Offset.timeBasedUUID(timeBasedUuid),
    timestampOffset,
    TimestampOffset(Instant.parse("2026-10-18T12:34:56Z"), Map.empty),
    TagOffsets(
      Map(
        "tag-a"       -> Offset.sequence(3L),
        "odd=&:tag;%" -> timestampOffset,
        "tag-c"       -> NoOffset,
        "tag-d"       -> Offset.timeBasedUUID(timeBasedUuid)
      )
    ),
    TagOffsets(Map.empty)
  )
}

class OffsetCodecSpec extends AnyFlatSpec with Matchers {
  import OffsetCodecSpec._

  behavior of "OffsetCodec"

  it should "read back every offset type it writes" in {
    allOffsetTypes.foreach { offset =>
      val encoded = OffsetCodec.encode(offset)
      OffsetCodec.decode(encoded.offsetType, encoded.value) shouldBe offset
    }
  }

  it should "read back every offset type it writes as a single string" in {
    allOffsetTypes.foreach { offset =>
      OffsetCodec.decodeString(OffsetCodec.encodeString(offset)) shouldBe offset
    }
  }

  it should "read the records CassandraOffsetStore used to write" in {
    OffsetCodec.decode("sequence", "42") shouldBe Offset.sequence(42L)
    OffsetCodec.decode("uuid", timeBasedUuid.toString) shouldBe Offset.timeBasedUUID(timeBasedUuid)
  }

  it should "read the records JdbcOffsetStore used to write" in {
    OffsetCodec.decode("NoOffset", "0") shouldBe NoOffset
    OffsetCodec.decode("Sequence", "42") shouldBe Offset.sequence(42L)
    OffsetCodec.decode("TimeBasedUUID", timeBasedUuid.toString) shouldBe Offset.timeBasedUUID(timeBasedUuid)
  }

  it should "read the records LmdbOffsetStore used to write" in {
    OffsetCodec.decodeString("sequence:42") shouldBe Offset.sequence(42L)
    OffsetCodec.decodeString(s"uuid:$timeBasedUuid") shouldBe Offset.timeBasedUUID(timeBasedUuid)
  }

  it should "read blank, unknown and unreadable records as NoOffset" in {
    OffsetCodec.decode("", "") shouldBe NoOffset
    OffsetCodec.decode(null, null) shouldBe NoOffset
    OffsetCodec.decode("somethingelse", "42") shouldBe NoOffset
    OffsetCodec.decode("sequence", "not-a-number") shouldBe NoOffset
    OffsetCodec.decode("uuid", "not-a-uuid") shouldBe NoOffset
    OffsetCodec.decode("timestamp", "yesterday") shouldBe NoOffset
    OffsetCodec.decode("tags", "tag-a=sequence%3Aabc") shouldBe NoOffset
    OffsetCodec.decodeString("") shouldBe NoOffset
    OffsetCodec.decodeString("garbage") shouldBe NoOffset
  }

  it should "refuse to write an offset type it does not know" in {
    an[IllegalArgumentException] should be thrownBy OffsetCodec.encode(new Offset {})
  }
}
