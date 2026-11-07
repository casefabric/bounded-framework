/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.projection

import org.apache.pekko.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.apache.pekko.actor.typed.ActorRef
import org.apache.pekko.actor.typed.scaladsl.AskPattern._
import org.apache.pekko.projection.{HandlerRecoveryStrategy, ProjectionBehavior}
import org.apache.pekko.projection.jdbc.JdbcSession
import org.apache.pekko.projection.jdbc.scaladsl.JdbcProjection
import com.typesafe.config.ConfigFactory
import io.cafienne.bounded.aggregate.{EventPosition, Ok, Reply}
import io.cafienne.bounded.cluster.Notes
import io.cafienne.bounded.cluster.Notes._
import io.cafienne.bounded.readside.{Failed, Pending, Processed, ReadSideRegistry}
import org.scalatest.wordspec.AnyWordSpecLike

import java.sql.{Connection, DriverManager}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration._
import scala.util.Using

object ReadSideProjectionSpec {
  val database = "jdbc:h2:mem:bounded-read-side;DB_CLOSE_DELAY=-1"

  val config = ConfigFactory
    .parseString("""
      pekko.loglevel = INFO
      pekko.persistence.journal.plugin = "inmemory-journal"
      pekko.persistence.snapshot-store.plugin = "inmemory-snapshot-store"
      inmemory-read-journal.refresh-interval = 10ms
      bounded.eventmaterializers.publish = on
      pekko.projection.jdbc {
        dialect = "h2-dialect"
        blocking-jdbc-dispatcher.thread-pool-executor.fixed-pool-size = 4
      }
      # a failed projection restarts quickly
      pekko.projection.restart-backoff {
        min-backoff = 100ms
        max-backoff = 200ms
      }
    """)
    .withFallback(ConfigFactory.load())

  final class H2Session extends JdbcSession {
    private val connection = {
      val created = DriverManager.getConnection(database)
      created.setAutoCommit(false)
      created
    }
    override def withConnection[Result](func: org.apache.pekko.japi.function.Function[Connection, Result]): Result =
      func(connection)
    override def commit(): Unit   = connection.commit()
    override def rollback(): Unit = connection.rollback()
    override def close(): Unit    = connection.close()
  }
}

/**
  * Waiting for a Bounded projection with the `ReadSideRegistry`: the projection publishes its progress after the
  * commit, also for filtered events, and its failures by recovery strategy.
  */
class ReadSideProjectionSpec extends ScalaTestWithActorTestKit(ReadSideProjectionSpec.config) with AnyWordSpecLike {
  import ReadSideProjectionSpec._

  private val registry   = ReadSideRegistry(system.classicSystem)
  private val aggregates = new NotesManager()
  private val counter    = new AtomicInteger()

  JdbcProjection.createTablesIfNotExists(() => new H2Session()).futureValue
  execute("CREATE TABLE IF NOT EXISTS notes_view (projection VARCHAR(100), note VARCHAR(100))")

  private def execute(statement: String): Unit =
    Using.resource(DriverManager.getConnection(database))(_.createStatement().execute(statement): Unit)

  /** Adds a note on an aggregate of its own; the note is its first event. */
  private def addNote(note: String): EventPosition = {
    val id                                = s"${note}-${counter.incrementAndGet()}"
    val aggregate: ActorRef[NotesCommand] = system.systemActorOf(aggregates.behaviorFor(id), s"aggregate-$id")
    aggregate.ask[Reply](AddNote(id, note, _)).futureValue shouldBe an[Ok]
    EventPosition(Notes.Type.persistenceId(id).id, 1)
  }

  private def rows(projection: String, note: String): Int =
    Using.resource(DriverManager.getConnection(database)) { connection =>
      val statement = connection.prepareStatement("SELECT count(*) FROM notes_view WHERE projection = ? AND note = ?")
      statement.setString(1, projection)
      statement.setString(2, note)
      val result = statement.executeQuery()
      result.next()
      result.getInt(1)
    }

  /**
    * A JDBC exactly-once projection that writes the notes of this test (those starting with `prefix`) and fails on
    * them as `fails(note, attempt)` says. Notes starting with "skip" are filtered out.
    */
  private def run(
    name: String,
    prefix: String,
    strategy: HandlerRecoveryStrategy,
    fails: (String, Int) => Boolean = (_, _) => false
  ): (EventProjection, ActorRef[ProjectionBehavior.Command]) = {
    val view       = BoundedProjectionsSpec.notesProjection(name, BoundedProjectionsSpec.SkipFilter)
    val attempts   = new ConcurrentHashMap[String, AtomicInteger]()
    val projection =
      BoundedJdbcProjection.exactlyOnce(system, view, () => new H2Session(), _.withRecoveryStrategy(strategy)) {
        (session, event) =>
          val note = BoundedProjectionsSpec.noteOf(event)
          if (note.startsWith(prefix)) {
            val attempt = attempts.computeIfAbsent(note, _ => new AtomicInteger()).incrementAndGet()
            if (fails(note, attempt)) throw new RuntimeException(s"cannot write $note")
            session.withConnection { connection =>
              val insert = connection.prepareStatement("INSERT INTO notes_view (projection, note) VALUES (?, ?)")
              insert.setString(1, name)
              insert.setString(2, note)
              insert.executeUpdate(): Unit
            }
          }
      }
    (view, BoundedProjections.runLocally(system, projection))
  }

  "A Bounded projection as read side" should {
    "let a waiter complete once the event is committed" in {
      val (view, running) = run("wait-commit", "commit", HandlerRecoveryStrategy.fail)
      val position        = addNote("commit-one")
      registry.awaitProcessed(view.readSideId, position, 10.seconds).futureValue shouldBe Processed
      rows("wait-commit", "commit-one") shouldBe 1
      running ! ProjectionBehavior.Stop
    }

    "let a waiter complete for an event the filter rejects" in {
      val (view, running) = run("wait-filtered", "skip", HandlerRecoveryStrategy.fail)
      val position        = addNote("skip-this")
      registry.awaitProcessed(view.readSideId, position, 10.seconds).futureValue shouldBe Processed
      rows("wait-filtered", "skip-this") shouldBe 0
      running ! ProjectionBehavior.Stop
    }

    "answer Processed when a retry succeeds" in {
      val (view, running) =
        run("wait-retry", "retry", HandlerRecoveryStrategy.retryAndFail(2, 50.millis), (_, attempt) => attempt == 1)
      val position = addNote("retry-one")
      registry.awaitProcessed(view.readSideId, position, 10.seconds).futureValue shouldBe Processed
      rows("wait-retry", "retry-one") shouldBe 1
      running ! ProjectionBehavior.Stop
    }

    "answer Failed after the last retry, and Pending(Down) to a waiter for another entity behind it" in {
      val (view, running) =
        run("wait-fail", "bad", HandlerRecoveryStrategy.retryAndFail(2, 50.millis), (note, _) => note == "bad-one")
      val failing   = addNote("bad-one")
      val behind    = addNote("bad-two")
      val atFailing = registry.awaitProcessed(view.readSideId, failing, 10.seconds)
      val atBehind  = registry.awaitProcessed(view.readSideId, behind, 10.seconds)
      atFailing.futureValue match {
        case Failed(cause) => cause.getMessage shouldBe "cannot write bad-one"
        case other         => fail(s"Unexpected $other")
      }
      atBehind.futureValue shouldBe a[Pending]
      atBehind.futureValue match {
        case Pending(Pending.Down(_)) => succeed
        case other                    => fail(s"Unexpected $other")
      }
      running ! ProjectionBehavior.Stop
    }

    "answer Failed for an event the strategy skips" in {
      val (view, running) = run("wait-skip", "lost", HandlerRecoveryStrategy.skip, (note, _) => note == "lost-one")
      val skipped         = addNote("lost-one")
      val next            = addNote("lost-two")
      registry.awaitProcessed(view.readSideId, skipped, 10.seconds).futureValue shouldBe a[Failed]
      registry.awaitProcessed(view.readSideId, next, 10.seconds).futureValue shouldBe Processed
      rows("wait-skip", "lost-one") shouldBe 0
      running ! ProjectionBehavior.Stop
    }

    "fail the future for a projection that was never built" in {
      registry
        .awaitProcessed("never-built-notes", EventPosition("notes|x", 1))
        .failed
        .futureValue shouldBe an[IllegalArgumentException]
    }
  }

  "BoundedProjections.retriesOf" should {
    "read the retries of each recovery strategy" in {
      BoundedProjections.retriesOf(HandlerRecoveryStrategy.fail) shouldBe 0
      BoundedProjections.retriesOf(HandlerRecoveryStrategy.skip) shouldBe 0
      BoundedProjections.retriesOf(HandlerRecoveryStrategy.retryAndFail(3, 1.second)) shouldBe 3
      BoundedProjections.retriesOf(HandlerRecoveryStrategy.retryAndSkip(5, 1.second)) shouldBe 5
    }
  }
}
