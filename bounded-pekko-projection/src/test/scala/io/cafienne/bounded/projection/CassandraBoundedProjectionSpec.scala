/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.projection

import org.apache.pekko.Done
import org.apache.pekko.actor.typed.{ActorRef, ActorSystem}
import org.apache.pekko.actor.typed.scaladsl.AskPattern._
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.projection.cassandra.scaladsl.CassandraProjection
import org.apache.pekko.projection.testkit.scaladsl.ProjectionTestKit
import org.apache.pekko.util.Timeout
import com.typesafe.config.ConfigFactory
import io.cafienne.bounded.aggregate.{Ok, Reply}
import io.cafienne.bounded.cluster.Notes._
import io.cafienne.bounded.eventmaterializers.offsetstores.DockerTests
import io.cafienne.bounded.migration.CassandraAggregateTypeMigrationSpec
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec
import org.testcontainers.cassandra.CassandraContainer

import java.util.concurrent.ConcurrentLinkedQueue
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

/**
  * BoundedCassandraProjection, at least once on the events of an aggregate type by tag, on Cassandra in a container;
  * canceled when Docker is not available.
  */
class CassandraBoundedProjectionSpec extends AnyWordSpec with Matchers with ScalaFutures with BeforeAndAfterAll {
  import BoundedProjectionsSpec.{noteOf, notesProjection}

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(60, Seconds), interval = Span(200, Millis))
  implicit val timeout: Timeout = Timeout(30.seconds)

  private var cassandra: Option[CassandraContainer]     = None
  private var actorSystem: Option[ActorSystem[Nothing]] = None

  private lazy val system = {
    val container = new CassandraContainer("cassandra:4.1")
    container.start()
    cassandra = Some(container)
    val config = ConfigFactory
      .parseString("""
        pekko.actor.serializers.notes = "io.cafienne.bounded.cluster.NotesSerializer"
        pekko.actor.serialization-bindings {
          "io.cafienne.bounded.cluster.Notes$NotesMessage" = notes
        }
      """)
      .withFallback(CassandraAggregateTypeMigrationSpec.config(container.getContactPoint, container.getLocalDatacenter))
    val created = ActorSystem[Nothing](Behaviors.empty[Nothing], "CassandraBoundedProjectionSpec", config)
    actorSystem = Some(created)
    CassandraProjection.createTablesIfNotExists()(created).futureValue
    created
  }

  private def addNotes(notes: String*): Unit = notes.foreach { note =>
    implicit val typedSystem: ActorSystem[Nothing] = system
    val aggregate: ActorRef[NotesCommand]          =
      system.systemActorOf(new NotesManager().behaviorFor(s"cassandra-$note"), s"cassandra-$note")
    aggregate.ask[Reply](AddNote(s"cassandra-$note", note, _)).futureValue shouldBe an[Ok]
  }

  "A Bounded read model run as a Cassandra projection" should {
    "handle the events at least once, skipping filtered events, and resume from its stored offset" in {
      assume(DockerTests.dockerAvailable, "Docker is not available")
      val view         = notesProjection("at-least-once-cassandra")
      val handled      = new ConcurrentLinkedQueue[String]()
      def projection() =
        BoundedCassandraProjection.atLeastOnce(system, view, _.withSaveOffset(1, Duration.Zero)) { event =>
          handled.add(noteOf(event))
          Future.successful(Done)
        }

      addNotes("one", "skip-two", "three")
      ProjectionTestKit(system).run(projection(), 60.seconds) {
        handled.asScala.toList shouldBe List("one", "three"): Unit
      }

      addNotes("four")
      ProjectionTestKit(system).run(projection(), 60.seconds) {
        handled.asScala.toList shouldBe List("one", "three", "four"): Unit
      }
    }
  }

  override protected def afterAll(): Unit = {
    actorSystem.foreach { created =>
      created.terminate()
      Await.result(created.whenTerminated, 30.seconds)
    }
    cassandra.foreach(_.stop())
  }
}
