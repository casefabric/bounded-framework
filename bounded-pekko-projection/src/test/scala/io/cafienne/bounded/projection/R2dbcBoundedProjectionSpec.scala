/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.projection

import org.apache.pekko.Done
import org.apache.pekko.actor.typed.{ActorRef, ActorSystem}
import org.apache.pekko.actor.typed.scaladsl.AskPattern._
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.projection.{HandlerRecoveryStrategy, ProjectionBehavior}
import org.apache.pekko.projection.testkit.scaladsl.ProjectionTestKit
import org.apache.pekko.util.Timeout
import com.typesafe.config.ConfigFactory
import io.cafienne.bounded.R2dbcPostgres
import io.cafienne.bounded.aggregate.{Ok, Reply}
import io.cafienne.bounded.cluster.Notes._
import io.cafienne.bounded.eventmaterializers.offsetstores.DockerTests
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.io.Source
import scala.jdk.CollectionConverters._
import scala.util.Using

/**
  * BoundedR2dbcProjection, exactly once on the events of an aggregate type by slices, on Postgres in a container;
  * canceled when Docker is not available.
  */
class R2dbcBoundedProjectionSpec
    extends AnyWordSpec
    with Matchers
    with ScalaFutures
    with Eventually
    with BeforeAndAfterAll {
  import BoundedProjectionsSpec.{noteOf, notesProjection}

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(30, Seconds), interval = Span(100, Millis))
  implicit val timeout: Timeout = Timeout(10.seconds)

  private var database: Option[R2dbcPostgres]           = None
  private var actorSystem: Option[ActorSystem[Nothing]] = None

  private lazy val postgres = {
    val created = new R2dbcPostgres()
    database = Some(created)
    val ddl = Using.resource(Source.fromResource("r2dbc/create_projection_tables_postgres.sql"))(_.mkString)
    ddl.split(";").map(_.trim).filter(_.nonEmpty).foreach(statement => created.update(statement))
    created
  }

  private lazy val system = {
    val config = ConfigFactory
      .parseString("""
        pekko.actor.serializers.notes = "io.cafienne.bounded.cluster.NotesSerializer"
        pekko.actor.serialization-bindings {
          "io.cafienne.bounded.cluster.Notes$NotesMessage" = notes
        }
      """)
      .withFallback(postgres.config)
    val created = ActorSystem[Nothing](Behaviors.empty[Nothing], "R2dbcBoundedProjectionSpec", config)
    actorSystem = Some(created)
    created
  }

  private def addNotes(notes: String*): Unit = notes.foreach { note =>
    implicit val typedSystem: ActorSystem[Nothing] = system
    val aggregate: ActorRef[NotesCommand]          =
      system.systemActorOf(new NotesManager().behaviorFor(s"r2dbc-$note"), s"r2dbc-$note")
    aggregate.ask[Reply](AddNote(s"r2dbc-$note", note, _)).futureValue shouldBe an[Ok]
  }

  "A Bounded read model run as an R2DBC projection" should {
    "commit the statements and the offset of an exactly once projection together, skipping filtered events" in {
      assume(DockerTests.dockerAvailable, "Docker is not available")
      addNotes("one", "skip-two", "three")
      val view       = notesProjection("exactly-once-r2dbc")
      val failOnce   = new AtomicBoolean(true)
      val projection = BoundedR2dbcProjection.exactlyOnce(
        system,
        view,
        adjust = _.withRecoveryStrategy(HandlerRecoveryStrategy.retryAndFail(3, 100.millis))
      ) { (session, event) =>
        val note   = noteOf(event)
        val insert = session
          .createStatement("INSERT INTO notes_view (projection, note) VALUES ($1, $2)")
          .bind(0, view.projectionName)
          .bind(1, note)
        session
          .updateOne(insert)
          .map { _ =>
            // fails after its write: the write is rolled back with the offset
            if (note == "three" && failOnce.getAndSet(false)) throw new RuntimeException("failing once")
            Done
          }(system.executionContext)
      }

      ProjectionTestKit(system).run(projection, 30.seconds) {
        postgres.query("SELECT note FROM notes_view WHERE projection = ?", view.projectionName)(_.getString(1)) should
          contain theSameElementsAs List("one", "three"): Unit
      }
      failOnce.get() shouldBe false
    }

    "split a read model over slice ranges, one projection per range" in {
      assume(DockerTests.dockerAvailable, "Docker is not available")
      val view    = notesProjection("sliced-r2dbc")
      val handled = new ConcurrentLinkedQueue[String]()
      val ranges  = BoundedR2dbcProjection.sliceRanges(system, 2)
      ranges should have size 2
      val projections = ranges.map { range =>
        BoundedR2dbcProjection.atLeastOnceAsync(system, view, slices = Some(range)) { event =>
          handled.add(noteOf(event))
          Future.successful(Done)
        }
      }
      projections.map(_.projectionId.key) shouldBe ranges.map(range =>
        s"${view.aggregateType.name}-${range.min}-${range.max}"
      )

      val running = projections.map(BoundedProjections.runLocally(system, _))
      try eventually(timeout(30.seconds))(handled.asScala.toList should contain theSameElementsAs List("one", "three"))
      finally running.foreach(_ ! ProjectionBehavior.Stop)
    }
  }

  override protected def afterAll(): Unit = {
    actorSystem.foreach { created =>
      created.terminate()
      Await.result(created.whenTerminated, 30.seconds)
    }
    database.foreach(_.stop())
  }
}
