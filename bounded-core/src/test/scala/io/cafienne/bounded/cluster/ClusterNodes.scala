/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.cluster

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.cluster.MemberStatus
import org.apache.pekko.cluster.typed.{Cluster, Join}
import org.apache.pekko.persistence.jdbc.testkit.scaladsl.SchemaUtils
import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * Actor systems that form a cluster in one JVM, for the cluster specs: Java serialization is off, so everything that
  * crosses nodes needs a serializer, and all nodes share one journal (the JDBC journal on an in-memory H2 database).
  */
object ClusterNodes {

  /**
    * The config of a node of the cluster `name`; nodes with the same name share a journal.
    */
  def config(name: String, extra: String = ""): Config =
    ConfigFactory
      .parseString(extra)
      .withFallback(ConfigFactory.parseString(s"""
        pekko.loglevel = INFO
        pekko.actor.provider = cluster
        pekko.remote.artery.canonical.hostname = "127.0.0.1"
        pekko.remote.artery.canonical.port = 0
        pekko.actor.allow-java-serialization = off
        pekko.actor.warn-about-java-serializer-usage = on
        pekko.actor.serializers.notes = "io.cafienne.bounded.cluster.NotesSerializer"
        pekko.actor.serialization-bindings {
          "io.cafienne.bounded.cluster.Notes$$NotesMessage" = notes
        }
        pekko.cluster.jmx.multi-mbeans-in-same-jvm = on
        pekko.cluster.sharding.distributed-data.durable.keys = []
        pekko.coordinated-shutdown.exit-jvm = off
        pekko.persistence.journal.plugin = "jdbc-journal"
        pekko.persistence.snapshot-store.plugin = "jdbc-snapshot-store"
        pekko-persistence-jdbc.shared-databases.slick {
          profile = "slick.jdbc.H2Profile$$"
          db {
            url = "jdbc:h2:mem:$name;DB_CLOSE_DELAY=-1"
            driver = "org.h2.Driver"
            numThreads = 4
            maxConnections = 4
            minConnections = 1
          }
        }
        jdbc-journal.use-shared-db = "slick"
        jdbc-snapshot-store.use-shared-db = "slick"
        jdbc-read-journal.use-shared-db = "slick"
        jdbc-read-journal.refresh-interval = 100ms
      """))
      .withFallback(ConfigFactory.load())

  /**
    * Starts `count` nodes of the cluster `name` and waits until they are all up. The first node creates the journal
    * tables.
    */
  def start(name: String, count: Int, extra: String = ""): Vector[ActorSystem[Nothing]] = {
    val nodes = Vector.fill(count)(ActorSystem[Nothing](Behaviors.empty[Nothing], name, config(name, extra)))
    Await.result(SchemaUtils.createIfNotExists()(nodes.head), 30.seconds)
    val seed = Cluster(nodes.head).selfMember.address
    nodes.foreach(node => Cluster(node).manager ! Join(seed))
    awaitUp(nodes, count)
    nodes
  }

  /** Waits until every node in `nodes` sees `count` members up. */
  def awaitUp(nodes: Seq[ActorSystem[?]], count: Int, within: FiniteDuration = 30.seconds): Unit = {
    val deadline = within.fromNow
    def allUp    = nodes.forall { node =>
      val members = Cluster(node).state.members
      members.size == count && members.forall(_.status == MemberStatus.Up)
    }
    while (!allUp) {
      if (deadline.isOverdue()) throw new AssertionError(s"The cluster did not get $count members up within $within")
      Thread.sleep(100)
    }
  }

  /** Stops the nodes; each leaves the cluster first. */
  def stop(nodes: Seq[ActorSystem[?]]): Unit = {
    nodes.foreach(_.terminate())
    nodes.foreach(node => Await.result(node.whenTerminated, 30.seconds))
  }
}
