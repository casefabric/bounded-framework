/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.eventmaterializers.offsetstores

import org.apache.pekko.persistence.query.{NoOffset, Offset}
import com.typesafe.config.ConfigFactory
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import slick.basic.DatabaseConfig
import slick.jdbc.JdbcProfile

class JdbcOffsetStoreSpec extends AnyFlatSpec with Matchers with ScalaFutures {
  import OffsetCodecSpec._

  implicit val patience: PatienceConfig = PatienceConfig(timeout = Span(10, Seconds), interval = Span(50, Millis))

  val config = ConfigFactory.parseString("""
    offsetstore-h2 {
      profile = "slick.jdbc.H2Profile$"
      db {
        url = "jdbc:h2:mem:jdbc-offsetstore-spec;DB_CLOSE_DELAY=-1"
        driver = "org.h2.Driver"
        connectionPool = disabled
        keepAliveConnection = true
      }
    }
  """)

  lazy val store = new JdbcOffsetStore(DatabaseConfig.forConfig[JdbcProfile]("offsetstore-h2", config))

  behavior of "JdbcOffsetStore"

  it should "create its table on first use and store and read back every offset type" in {
    allOffsetTypes.zipWithIndex.foreach {
      case (offset, i) =>
        store.saveOffset(s"view-$i", offset).futureValue
        store.getOffset(s"view-$i").futureValue shouldBe offset
    }
  }

  it should "overwrite the offset of a view" in {
    store.saveOffset("view-overwrite", Offset.sequence(1L)).futureValue
    store.saveOffset("view-overwrite", Offset.sequence(2L)).futureValue

    store.getOffset("view-overwrite").futureValue shouldBe Offset.sequence(2L)
  }

  it should "read a record written by an older version" in {
    import store.dbConfig.profile.api._
    store.getOffset("view-legacy").futureValue shouldBe NoOffset
    store.dbConfig.db.run(store.OffsetStore += (("view-legacy", "42", "Sequence"))).futureValue

    store.getOffset("view-legacy").futureValue shouldBe Offset.sequence(42L)
  }

  it should "clear the offset of one view and of all views" in {
    store.saveOffset("view-clear-1", Offset.sequence(1L)).futureValue
    store.saveOffset("view-clear-2", Offset.sequence(2L)).futureValue

    store.clear("view-clear-1").futureValue
    store.getOffset("view-clear-1").futureValue shouldBe NoOffset
    store.getOffset("view-clear-2").futureValue shouldBe Offset.sequence(2L)

    store.clear().futureValue
    store.getOffset("view-clear-2").futureValue shouldBe NoOffset
  }

  behavior of "JdbcOffsetStore database config path"

  it should "use the slick config of the journal plugin by default" in {
    JdbcOffsetStore.databaseConfigPath(ConfigFactory.empty(), "jdbc-journal") shouldBe "jdbc-journal.slick"
  }

  it should "still read the old key" in {
    val config = ConfigFactory.parseString("""pekko.persistence.offset.r2dbc.store = "my-db"""")

    JdbcOffsetStore.databaseConfigPath(config, "jdbc-journal") shouldBe "my-db"
  }

  it should "prefer the bounded key" in {
    val config = ConfigFactory.parseString("""
      pekko.persistence.offset.r2dbc.store = "my-db"
      bounded.eventmaterializers.jdbc-offsetstore.database-config = "offsets-db"
    """)

    JdbcOffsetStore.databaseConfigPath(config, "jdbc-journal") shouldBe "offsets-db"
  }
}
