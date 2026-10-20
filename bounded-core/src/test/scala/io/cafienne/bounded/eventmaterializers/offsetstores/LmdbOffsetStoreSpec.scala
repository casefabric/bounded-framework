/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.eventmaterializers.offsetstores

import java.nio.file.Files

import org.apache.pekko.persistence.query.{NoOffset, Offset}
import com.typesafe.config.ConfigFactory
import io.cafienne.bounded.eventmaterializers.TagOffsets
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class LmdbOffsetStoreSpec extends AnyFlatSpec with Matchers with ScalaFutures {
  import OffsetCodecSpec._

  // LmdbOffsetStore is a JVM-wide singleton; no other spec uses it.
  lazy val store: OffsetStore = {
    val path = Files.createTempDirectory("lmdb-offsetstore-spec").resolve("offsets").toString
    LmdbOffsetStore(new LmdbConfig(ConfigFactory.parseString(s"""path = "$path"""")))
  }

  behavior of "LmdbOffsetStore"

  it should "store and read back every offset type" in {
    allOffsetTypes.zipWithIndex.foreach {
      case (offset, i) =>
        store.saveOffset(s"view-$i", offset).futureValue
        store.getOffset(s"view-$i").futureValue shouldBe offset
    }
  }

  it should "store an offset value longer than 300 bytes" in {
    val manyTags = TagOffsets((1 to 50).map(i => s"some-longer-tag-name-$i" -> Offset.sequence(i.toLong)).toMap)

    store.saveOffset("view-many-tags", manyTags).futureValue
    store.getOffset("view-many-tags").futureValue shouldBe manyTags
  }

  it should "store an offset under a view identifier longer than 100 bytes" in {
    val longView = "v" * 200

    store.saveOffset(longView, Offset.sequence(9L)).futureValue
    store.getOffset(longView).futureValue shouldBe Offset.sequence(9L)
  }

  it should "refuse a view identifier longer than LMDB's key limit with a clear error" in {
    val tooLong = "v" * 600

    store.saveOffset(tooLong, Offset.sequence(1L)).failed.futureValue.getMessage should include(
      "LMDB offset store allows"
    )
    store.getOffset(tooLong).failed.futureValue shouldBe an[IllegalArgumentException]
  }

  it should "read NoOffset for a view without a stored offset" in {
    store.getOffset("view-unknown").futureValue shouldBe NoOffset
  }
}
