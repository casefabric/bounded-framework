/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.eventmaterializers.offsetstores

import com.typesafe.config.ConfigFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class CassandraOffsetStoreSpec extends AnyFlatSpec with Matchers {

  val pekkoJournalPlugin = "pekko.persistence.cassandra.journal"

  behavior of "CassandraOffsetStore keyspace"

  it should "use the keyspace of the Pekko Cassandra journal plugin by default" in {
    // the plugin's reference.conf is on the classpath and sets the keyspace to "pekko"
    CassandraOffsetStore.keyspace(ConfigFactory.load(), pekkoJournalPlugin) shouldBe "pekko"
  }

  it should "use a keyspace configured for the journal plugin" in {
    val config = ConfigFactory
      .parseString(s"""$pekkoJournalPlugin.keyspace = "myapp"""")
      .withFallback(ConfigFactory.load())

    CassandraOffsetStore.keyspace(config, pekkoJournalPlugin) shouldBe "myapp"
  }

  it should "prefer the bounded offset store keyspace when it is set" in {
    val config = ConfigFactory
      .parseString(s"""${CassandraOffsetStore.KeyspaceConfigPath} = "akka"""")
      .withFallback(ConfigFactory.load())

    CassandraOffsetStore.keyspace(config, pekkoJournalPlugin) shouldBe "akka"
  }

  it should "read the keyspace of any configured journal plugin path" in {
    val config = ConfigFactory.parseString("""cassandra-journal.keyspace = "legacy"""")

    CassandraOffsetStore.keyspace(config, "cassandra-journal") shouldBe "legacy"
  }

  behavior of "CassandraOffsetStore statements"

  they should "all use the same keyspace" in {
    val statements = Seq(
      CassandraOffsetStore.createTableStatement("myapp"),
      CassandraOffsetStore.insertStatement("myapp"),
      CassandraOffsetStore.selectStatement("myapp")
    )

    all(statements) should include("myapp.vw_offsetstore")
    all(statements) should not include "akka"
  }

  "the select statement" should "bind the view identifier instead of embedding it" in {
    CassandraOffsetStore.selectStatement("myapp") should endWith("WHERE view_identifier = ?")
  }
}
