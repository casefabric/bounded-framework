/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.projection

import org.apache.pekko.Done
import org.apache.pekko.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.apache.pekko.actor.typed.{ActorRef, ActorSystem}
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.scaladsl.AskPattern._
import org.apache.pekko.actor.typed.eventstream.EventStream
import org.apache.pekko.cluster.typed.{Cluster, Join}
import org.apache.pekko.persistence.query.{Offset, PersistenceQuery, Sequence}
import org.apache.pekko.persistence.query.scaladsl.CurrentEventsByTagQuery
import org.apache.pekko.projection.HandlerRecoveryStrategy
import org.apache.pekko.projection.jdbc.JdbcSession
import org.apache.pekko.projection.jdbc.scaladsl.JdbcProjection
import org.apache.pekko.projection.slick.SlickProjection
import org.apache.pekko.projection.testkit.scaladsl.ProjectionTestKit
import org.apache.pekko.stream.scaladsl.Sink
import com.typesafe.config.ConfigFactory
import io.cafienne.bounded.aggregate.{DomainEvent, Ok, Reply}
import io.cafienne.bounded.cluster.Notes
import io.cafienne.bounded.cluster.Notes._
import io.cafienne.bounded.eventmaterializers.{EventProcessed, MaterializerEventFilter, NoFilterEventFilter}
import org.scalatest.wordspec.AnyWordSpecLike
import slick.basic.DatabaseConfig
import slick.jdbc.H2Profile

import java.sql.{Connection, DriverManager}
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.Using

object BoundedProjectionsSpec {
  val database = "jdbc:h2:mem:bounded-projections;DB_CLOSE_DELAY=-1"

  val config = ConfigFactory
    .parseString(s"""
      pekko.loglevel = INFO
      pekko.actor.provider = cluster
      pekko.remote.artery.canonical.hostname = "127.0.0.1"
      pekko.remote.artery.canonical.port = 0
      pekko.cluster.jmx.multi-mbeans-in-same-jvm = on
      pekko.persistence.journal.plugin = "inmemory-journal"
      pekko.persistence.snapshot-store.plugin = "inmemory-snapshot-store"
      inmemory-read-journal.refresh-interval = 10ms
      bounded.eventmaterializers.publish = true
      pekko.projection.jdbc {
        dialect = "h2-dialect"
        blocking-jdbc-dispatcher.thread-pool-executor.fixed-pool-size = 4
      }
      slick-projections {
        profile = "slick.jdbc.H2Profile$$"
        db {
          url = "jdbc:h2:mem:bounded-projections-slick;DB_CLOSE_DELAY=-1"
          driver = "org.h2.Driver"
          connectionPool = disabled
          keepAliveConnection = true
        }
      }
    """)
    .withFallback(ConfigFactory.load())

  /** A JDBC session on its own connection, with a transaction per envelope (exactly once) or per batch. */
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

  /** Skips the notes that start with "skip". */
  object SkipFilter extends MaterializerEventFilter {
    override def filter(evt: DomainEvent): Boolean = evt match {
      case NoteAdded(_, note) => !note.startsWith("skip")
      case _                  => true
    }
  }

  def notesProjection(name: String, filter: MaterializerEventFilter = SkipFilter): EventProjection =
    new EventProjection {
      override val projectionName                       = name
      override val aggregateType                        = Notes.Type
      override val eventFilter: MaterializerEventFilter = filter
    }

  def noteOf(event: Any): String = event match {
    case NoteAdded(_, note) => note
    case other              => throw new IllegalArgumentException(s"Unexpected event $other")
  }
}

/**
  * Bounded handlers run as Pekko Projections on the in-memory journal: at least once and exactly once on JDBC (H2)
  * and Slick (H2), and once in a cluster.
  */
class BoundedProjectionsSpec extends ScalaTestWithActorTestKit(BoundedProjectionsSpec.config) with AnyWordSpecLike {
  import BoundedProjectionsSpec._

  private val projectionTestKit = ProjectionTestKit(system)
  private val slickDatabase     = DatabaseConfig.forConfig[H2Profile]("slick-projections", system.settings.config)
  private var written           = Vector.empty[String]

  Cluster(system).manager ! Join(Cluster(system).selfMember.address)
  JdbcProjection.createTablesIfNotExists(() => new H2Session()).futureValue
  SlickProjection.createTablesIfNotExists(slickDatabase).futureValue
  private val notesTable = "CREATE TABLE IF NOT EXISTS notes_view (projection VARCHAR(100), note VARCHAR(100))"
  execute(notesTable)
  locally {
    import slickDatabase.profile.api._
    slickDatabase.db.run(sqlu"#$notesTable").futureValue
  }

  private val aggregates = new NotesManager()

  private def addNotes(notes: String*): Unit = notes.foreach { note =>
    val aggregate: ActorRef[NotesCommand] =
      system.systemActorOf(aggregates.behaviorFor(s"aggregate-$note"), s"aggregate-$note")
    aggregate.ask[Reply](AddNote(s"aggregate-$note", note, _)).futureValue shouldBe an[Ok]
    written :+= note
  }

  private def execute(statement: String): Unit =
    Using.resource(DriverManager.getConnection(database))(_.createStatement().execute(statement): Unit)

  private def insert(view: EventProjection, note: String)(connection: Connection): Unit = {
    val statement = connection.prepareStatement("INSERT INTO notes_view (projection, note) VALUES (?, ?)")
    statement.setString(1, view.projectionName)
    statement.setString(2, note)
    statement.executeUpdate(): Unit
  }

  private def rows(projection: String): List[String] =
    Using.resource(DriverManager.getConnection(database)) { connection =>
      val statement = connection.prepareStatement("SELECT note FROM notes_view WHERE projection = ?")
      statement.setString(1, projection)
      val result = statement.executeQuery()
      Iterator.continually(result).takeWhile(_.next()).map(_.getString(1)).toList
    }

  private def storedOffset(projection: EventProjection): Option[String] =
    Using.resource(DriverManager.getConnection(database)) { connection =>
      val statement = connection.prepareStatement(
        """SELECT "current_offset" FROM "pekko_projection_offset_store" WHERE "projection_name" = ? AND "projection_key" = ?"""
      )
      statement.setString(1, projection.projectionName)
      statement.setString(2, projection.aggregateType.name)
      val result = statement.executeQuery()
      if (result.next()) Some(result.getString(1)) else None
    }

  private def lastOffset(): Offset =
    PersistenceQuery(system)
      .readJournalFor[CurrentEventsByTagQuery]("inmemory-read-journal")
      .currentEventsByTag(Notes.Type.name, Offset.noOffset)
      .runWith(Sink.last)
      .futureValue
      .offset

  private def published(projection: EventProjection): ConcurrentLinkedQueue[String] = {
    val notes = new ConcurrentLinkedQueue[String]()
    val probe = spawn(Behaviors.receiveMessage[EventProcessed] { processed =>
      if (processed.materializerId == projection.materializerId) notes.add(noteOf(processed.evt)): Unit
      Behaviors.same
    })
    system.eventStream ! EventStream.Subscribe(probe)
    notes
  }

  "A Bounded handler run as a Pekko Projection" should {
    "handle the events at least once, store the offset of filtered events and publish EventProcessed" in {
      addNotes("one", "skip-two", "three", "skip-four")
      val view       = notesProjection("at-least-once")
      val handled    = new ConcurrentLinkedQueue[String]()
      val processed  = published(view)
      val projection =
        BoundedJdbcProjection.atLeastOnceAsync(
          system,
          view,
          () => new H2Session(),
          _.withSaveOffset(1, Duration.Zero)
        ) { event =>
          handled.add(noteOf(event))
          Future.successful(Done)
        }

      projectionTestKit.run(projection, 30.seconds) {
        handled.asScala.toList shouldBe List("one", "three")
        // the last event is filtered out, and its offset is still stored
        storedOffset(view) shouldBe Some(lastOffset().asInstanceOf[Sequence].value.toString): Unit
      }
      processed.asScala.toList shouldBe List("one", "three")
    }

    "commit the data and the offset of an exactly once JDBC projection together" in {
      addNotes("five")
      val view       = notesProjection("exactly-once-jdbc", NoFilterEventFilter)
      val failOnce   = new AtomicBoolean(true)
      val processed  = published(view)
      val projection = BoundedJdbcProjection.exactlyOnce(
        system,
        view,
        () => new H2Session(),
        _.withRecoveryStrategy(HandlerRecoveryStrategy.retryAndFail(3, 100.millis))
      ) { (session, event) =>
        session.withConnection { connection =>
          val insert = connection.prepareStatement("INSERT INTO notes_view (projection, note) VALUES (?, ?)")
          insert.setString(1, view.projectionName)
          insert.setString(2, noteOf(event))
          insert.executeUpdate()
        }
        // fails after its write: the write is rolled back with the offset
        if (noteOf(event) == "three" && failOnce.getAndSet(false)) throw new RuntimeException("failing once")
      }

      projectionTestKit.run(projection, 30.seconds) {
        rows(view.projectionName) should contain theSameElementsAs written: Unit
      }
      failOnce.get() shouldBe false
      processed.asScala.toList shouldBe written
    }

    "commit the action and the offset of an exactly once Slick projection together" in {
      import slickDatabase.profile.api._
      val view       = notesProjection("exactly-once-slick")
      val failOnce   = new AtomicBoolean(true)
      val projection = BoundedSlickProjection.exactlyOnce(
        system,
        view,
        slickDatabase,
        _.withRecoveryStrategy(HandlerRecoveryStrategy.retryAndFail(3, 100.millis))
      ) { event =>
        val note = noteOf(event)
        sqlu"INSERT INTO notes_view (projection, note) VALUES (${view.projectionName}, $note)".flatMap { _ =>
          if (note == "one" && failOnce.getAndSet(false)) DBIO.failed(new RuntimeException("failing once"))
          else DBIO.successful(Done)
        }(system.executionContext)
      }

      // the Slick projection writes to its own database
      projectionTestKit.run(projection, 30.seconds) {
        slickDatabase.db
          .run(sql"SELECT note FROM notes_view WHERE projection = ${view.projectionName}".as[String])
          .futureValue should contain theSameElementsAs written.filterNot(_.startsWith("skip")): Unit
      }
      failOnce.get() shouldBe false
    }
  }

  "The store-session and grouped variants" should {
    "write the events on the session at least once" in {
      val view       = notesProjection("at-least-once-jdbc-session", NoFilterEventFilter)
      val projection = BoundedJdbcProjection.atLeastOnce(system, view, () => new H2Session()) { (session, event) =>
        session.withConnection(insert(view, noteOf(event)))
      }

      projectionTestKit.run(projection, 30.seconds) {
        rows(view.projectionName) should contain theSameElementsAs written: Unit
      }
    }

    "hand over the events in groups without the filtered ones, in the transaction of the group's offset" in {
      addNotes("skip-seven", "eight")
      val view       = notesProjection("grouped-jdbc")
      val groups     = new ConcurrentLinkedQueue[Seq[String]]()
      val processed  = published(view)
      val accepted   = written.filterNot(_.startsWith("skip"))
      val projection = BoundedJdbcProjection.groupedWithin(
        system,
        view,
        () => new H2Session(),
        _.withGroup(100, 200.millis)
      ) { (session, events) =>
        groups.add(events.map(noteOf))
        events.foreach(event => session.withConnection(insert(view, noteOf(event))))
      }

      projectionTestKit.run(projection, 30.seconds) {
        rows(view.projectionName) should contain theSameElementsAs accepted: Unit
      }
      groups.asScala.flatten.toList should contain theSameElementsAs accepted
      groups.asScala.flatten.exists(_.startsWith("skip")) shouldBe false
      groups.asScala.exists(_.size > 1) shouldBe true
      processed.asScala.toList should contain theSameElementsAs accepted
    }
  }

  "The store projections without a timestamp offset store" should {
    "refuse the R2DBC journal and point to BoundedR2dbcProjection" in {
      val r2dbcSystem = ActorSystem[Nothing](
        Behaviors.empty[Nothing],
        "R2dbcJournalSystem",
        ConfigFactory
          .parseString("""pekko.persistence.journal.plugin = "pekko.persistence.r2dbc.journal"""")
          .withFallback(ConfigFactory.load())
      )
      try {
        val view    = notesProjection("on-r2dbc")
        val refusal = the[IllegalArgumentException] thrownBy
          BoundedJdbcProjection.exactlyOnce(r2dbcSystem, view, () => new H2Session())((_, _) => ())
        refusal.getMessage should include("BoundedR2dbcProjection")
        an[IllegalArgumentException] should be thrownBy
          BoundedSlickProjection.atLeastOnceAsync(r2dbcSystem, view, slickDatabase)(_ => Future.successful(Done))
        an[IllegalArgumentException] should be thrownBy
          BoundedCassandraProjection.atLeastOnce(r2dbcSystem, view)(_ => Future.successful(Done))
        // and slice ranges are for the R2DBC journal only
        an[IllegalArgumentException] should be thrownBy BoundedR2dbcProjection.sliceRanges(system, 2)
      } finally {
        r2dbcSystem.terminate()
        r2dbcSystem.whenTerminated.futureValue: Unit
      }
    }
  }

  "BoundedProjections.runInCluster" should {
    "run each projection once in the cluster" in {
      val views       = List(notesProjection("cluster-a"), notesProjection("cluster-b"))
      val handled     = views.map(view => view.projectionName -> new ConcurrentLinkedQueue[String]()).toMap
      val projections = views.map { view =>
        BoundedJdbcProjection.atLeastOnceAsync(system, view, () => new H2Session()) { event =>
          handled(view.projectionName).add(noteOf(event))
          Future.successful(Done)
        }
      }

      BoundedProjections.runInCluster(system, "bounded-projections", projections)
      addNotes("six")

      views.foreach { view =>
        eventually(timeout(30.seconds)) {
          handled(view.projectionName).asScala.toList should contain theSameElementsAs written.filterNot(
            _.startsWith("skip")
          )
        }
      }
    }
  }

  override protected def afterAll(): Unit = {
    slickDatabase.db.close()
    super.afterAll()
  }
}
