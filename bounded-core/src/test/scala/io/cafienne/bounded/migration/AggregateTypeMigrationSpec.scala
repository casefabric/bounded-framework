/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.migration

import org.apache.pekko.Done
import org.apache.pekko.actor.{ActorRef, ActorSystem, Props}
import org.apache.pekko.actor.typed.Scheduler
import org.apache.pekko.actor.typed.scaladsl.adapter._
import org.apache.pekko.pattern.ask
import org.apache.pekko.persistence.{DeleteMessagesSuccess, PersistentActor, SaveSnapshotSuccess}
import org.apache.pekko.persistence.jdbc.testkit.scaladsl.SchemaUtils
import org.apache.pekko.persistence.journal.Tagged
import org.apache.pekko.persistence.query.{NoOffset, Offset, PersistenceQuery, Sequence}
import org.apache.pekko.persistence.query.scaladsl.{
  CurrentEventsByPersistenceIdQuery,
  CurrentPersistenceIdsQuery,
  ReadJournal
}
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.testkit.{TestKit, TestProbe}
import org.apache.pekko.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.Logger
import io.cafienne.bounded.R2dbcPostgres
import io.cafienne.bounded.aggregate.{
  AggregateType,
  ClassicSimpleAggregate,
  Ko,
  SimpleAggregateManager,
  TestMetaData,
  TypedSimpleAggregate,
  ValidateableCommand
}
import io.cafienne.bounded.aggregate.ClassicSimpleAggregate._
import io.cafienne.bounded.aggregate.TypedSimpleAggregate.{AggregateCommandMetaData, SimpleAggregateCommand}
import io.cafienne.bounded.aggregate.typed.DefaultTypedCommandGateway
import io.cafienne.bounded.eventmaterializers.{AbstractReplayableEventMaterializer, SpecConfig, TagOffsets}
import io.cafienne.bounded.eventmaterializers.offsetstores.{DockerTests, InMemoryBasedOffsetStore}
import io.cafienne.bounded.pekko.ActorSystemProvider
import io.cafienne.bounded.pekko.persistence.{AggregateTypeReadJournal, ReadJournalProvider}
import org.scalatest.{Assertion, BeforeAndAfterAll}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.LoggerFactory
import org.testcontainers.cassandra.CassandraContainer

import java.net.InetSocketAddress
import java.time.{Instant, OffsetDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.immutable.ListMap
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

object AggregateTypeMigrationSpec {
  val Simple: AggregateType = ClassicSimpleAggregate.Type
  val Typed: AggregateType  = AggregateType(TypedSimpleAggregate.aggregateRootTag)

  val meta: TestMetaData = TestMetaData(OffsetDateTime.parse("2026-01-01T00:00:00Z"), None, None)

  // Classic events belong to the simple aggregate, typed ones to the typed aggregate, anything else to nothing.
  val rule: (String, Any) => Option[MigrationTarget] = AggregateTypeMigration.byEvent {
    case _: Created | _: ItemAdded                                           => Simple
    case _: TypedSimpleAggregate.Created | _: TypedSimpleAggregate.ItemAdded => Typed
  }

  final case class Append(events: Seq[Any], tags: Set[String] = Set.empty)
  final case class Snapshot(state: Any)
  final case class DeleteUpTo(sequenceNr: Long)

  /** Writes a stream the way Bounded 1.0.0 did: under any persistence id, with or without tags. */
  class LegacyStream(override val persistenceId: String) extends PersistentActor {
    private var replyTo: ActorRef = ActorRef.noSender

    override def receiveRecover: Receive = { case _ => () }

    override def receiveCommand: Receive = {
      case Append(events, tags) =>
        val sender = this.sender()
        persistAll(events.map(event => if (tags.isEmpty) event else Tagged(event, tags)))(_ => ())
        deferAsync(())(_ => sender ! Done)
      case Snapshot(state) =>
        replyTo = sender()
        saveSnapshot(state)
      case SaveSnapshotSuccess(_) => replyTo ! Done
      case DeleteUpTo(sequenceNr) =>
        replyTo = sender()
        deleteMessages(sequenceNr)
      case DeleteMessagesSuccess(_) => replyTo ! Done
    }
  }
}

/**
  * AggregateTypeMigration, run by every backend spec below on a journal of its own.
  */
abstract class AggregateTypeMigrationSpec extends AnyWordSpec with Matchers with ScalaFutures with BeforeAndAfterAll {
  import AggregateTypeMigrationSpec._

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(30, Seconds), interval = Span(100, Millis))
  implicit val timeout: Timeout = Timeout(10.seconds)

  /** The system on the journal to migrate. */
  def system: ActorSystem

  /** Whether the backend can run here; the tests are canceled when not. */
  def available: Boolean = true

  /** Whether the journal's tag queries still return deleted events. */
  def keepsDeletedEventsInTagQueries: Boolean

  private implicit def streamSystem: ActorSystem = system
  private lazy val migration                     = new AggregateTypeMigration(system, rule, batchSize = 2)
  private implicit lazy val ec: ExecutionContext = system.dispatcher
  private var report: Option[MigrationReport]    = None

  private def typeJournal: AggregateTypeReadJournal = {
    val specSystem = system
    new ActorSystemProvider with ReadJournalProvider {
      implicit def system: ActorSystem = specSystem
    }.aggregateTypeJournal
  }

  private def journal: ReadJournal with CurrentPersistenceIdsQuery with CurrentEventsByPersistenceIdQuery =
    PersistenceQuery(system)
      .readJournalFor[ReadJournal with CurrentPersistenceIdsQuery with CurrentEventsByPersistenceIdQuery](
        ReadJournalProvider.queryPluginId(system.settings.config.getString("pekko.persistence.journal.plugin"))
      )

  "AggregateTypeMigration" should {

    "report what it would do, without writing anything" in {
      assume(available, "The backend is not available")
      // a classic stream with a snapshot, its events tagged with the name the type gets (as an adapter could have)
      legacy("a", Append(Seq(Created("a"), ItemAdded("a", "a1")), tags = Set(Simple.name)))
      legacy("a", Snapshot(Some(ClassicSimpleAggregateState(List("a1")))))
      legacy("a", Append(Seq(ItemAdded("a", "a2")), tags = Set(Simple.name)))
      // a typed aggregate on PersistenceId.ofUniqueId
      legacy("b", Append(Seq(TypedSimpleAggregate.Created("b", meta), TypedSimpleAggregate.ItemAdded("b", meta, "b1"))))
      // two aggregate types sharing one stream, with a snapshot
      legacy("c", Append(Seq(Created("c"), TypedSimpleAggregate.Created("c", meta), ItemAdded("c", "c1"))))
      legacy("c", Snapshot(Some(ClassicSimpleAggregateState(List("c1")))))
      // a stream that is no aggregate
      legacy("not-an-aggregate", Append(Seq("hello")))
      // a classic stream whose events up to its snapshot were deleted
      legacy("e", Append(Seq(Created("e"), ItemAdded("e", "e1"), ItemAdded("e", "e2"))))
      legacy("e", Snapshot(Some(ClassicSimpleAggregateState(List("e1", "e2")))))
      legacy("e", Append(Seq(ItemAdded("e", "e3"))))
      legacy("e", DeleteUpTo(3L))

      val dryRun = migration.dryRun().futureValue
      val byId   = dryRun.streams.map(stream => stream.persistenceId -> stream).toMap

      byId("a") shouldBe StreamMigration("a", 3, ListMap("simple|a" -> 3L), 0, Some(2L))
      byId("a").copiesSnapshot shouldBe true
      byId("b") shouldBe StreamMigration("b", 2, ListMap(s"${Typed.name}|b" -> 2L), 0, None)
      byId("c") shouldBe StreamMigration("c", 3, ListMap("simple|c" -> 2L, s"${Typed.name}|c" -> 1L), 0, Some(3L))
      byId("c").split shouldBe true
      byId("c").copiesSnapshot shouldBe false
      byId("not-an-aggregate").migrates shouldBe false
      byId("e") shouldBe StreamMigration("e", 1, ListMap("simple|e" -> 1L), 0, Some(3L))
      persistenceIds().filter(AggregateType.of(_).isDefined) shouldBe empty
    }

    "copy the streams, so that every aggregate recovers the same state under type|id" in {
      assume(available, "The backend is not available")
      val migrated = migration.migrate().futureValue
      migrated.verified shouldBe true
      report = Some(migrated)

      classicItems("a") shouldBe Seq("a1", "a2")
      classicItems("c") shouldBe Seq("c1")
      classicItems("e") shouldBe Seq("e1", "e2", "e3") // e1 and e2 come from the copied snapshot
      typedItems("b") shouldBe List("b1")
      typedItems("c") shouldBe List()
      events("simple|c") shouldBe Seq(Created("c"), ItemAdded("c", "c1"))
      events("not-an-aggregate") shouldBe Seq("hello")
      persistenceIds() should not contain "not-an-aggregate|not-an-aggregate"
    }

    "continue an interrupted run without copying an event twice" in {
      assume(available, "The backend is not available")
      legacy("f", Append(Seq(Created("f"), ItemAdded("f", "f1"), ItemAdded("f", "f2"), ItemAdded("f", "f3"))))
      // an earlier run stopped after copying two events
      legacyUnder("simple|f", Append(Seq(Created("f"), ItemAdded("f", "f1")), tags = Set(Simple.name)))

      val migrated = migration.migrate().futureValue
      migrated.verified shouldBe true
      report = Some(migrated)

      events("simple|f") shouldBe Seq(Created("f"), ItemAdded("f", "f1"), ItemAdded("f", "f2"), ItemAdded("f", "f3"))
      events("simple|a") should have size 3
    }

    "not delete anything while a new stream is incomplete" in {
      assume(available, "The backend is not available")
      val incomplete = report.get.copy(streams = report.get.streams.map {
        case stream if stream.persistenceId == "a" => stream.copy(targets = ListMap("simple|a" -> 4L))
        case stream                                => stream
      })

      migration.deleteMigrated(incomplete).failed.futureValue shouldBe an[IllegalStateException]
      events("a") should have size 3
    }

    "delete the old streams, after which a materializer sees every event of a type once" in {
      assume(available, "The backend is not available")
      migration.deleteMigrated(report.get).futureValue shouldBe Done

      Seq("a", "b", "c", "e", "f").foreach(id => events(id) shouldBe empty)
      events("not-an-aggregate") shouldBe Seq("hello")
      classicItems("a") shouldBe Seq("a1", "a2")

      // the old events of a were tagged with the name of their new type
      val readable      = migration.oldStreamsReadableByType(Seq(Simple, Typed)).futureValue
      val stillReadable =
        if (keepsDeletedEventsInTagQueries) {
          readable shouldBe Map(Simple -> Set("a"))
          Seq(Created("a"), ItemAdded("a", "a1"), ItemAdded("a", "a2"))
        } else {
          readable shouldBe empty
          Nil
        }

      val view = new TypeView(Simple)
      view.replayEvents().futureValue
      view.handled should contain theSameElementsAs stillReadable ++ Seq(
        Created("a"),
        ItemAdded("a", "a1"),
        ItemAdded("a", "a2"),
        Created("c"),
        ItemAdded("c", "c1"),
        ItemAdded("e", "e3"),
        Created("f"),
        ItemAdded("f", "f1"),
        ItemAdded("f", "f2"),
        ItemAdded("f", "f3")
      )
    }

    "reset the offsets of event materializers" in {
      assume(available, "The backend is not available")
      val store = new InMemoryBasedOffsetStore()
      store.saveOffset("view-1", Sequence(5L)).futureValue
      store.saveOffset("view-2", Sequence(7L)).futureValue

      migration.resetOffsets(store, Seq("view-1", "view-2")).futureValue shouldBe Done

      store.getOffset("view-1").futureValue shouldBe NoOffset
      store.getOffset("view-2").futureValue shouldBe NoOffset
    }

    "start a materializer and a combined logic after the migrated events" in {
      assume(available, "The backend is not available")
      val store = new InMemoryBasedOffsetStore()
      // on R2DBC this waits until the backtracking window after the last copy has passed
      Await.result(migration.startAtEnd(store, "view-end", Simple), 3.minutes) shouldBe Done
      Await.result(
        migration.startCombinedLogicAtEnd(store, "combined-logic-end", Seq(Simple, Typed)),
        3.minutes
      ) shouldBe Done

      legacyUnder("simple|n", Append(Seq(Created("n")), tags = Set(Simple.name)))

      def liveFrom(aggregateType: AggregateType, offset: Offset): Seq[Any] =
        typeJournal
          .eventsByAggregateType(aggregateType, offset)
          .takeWithin(5.seconds)
          .map(_.event)
          .runWith(Sink.seq)
          .futureValue

      liveFrom(Simple, store.getOffset("view-end").futureValue) shouldBe Seq(Created("n"))
      store.getOffset("combined-logic-end").futureValue match {
        case TagOffsets(offsets) =>
          offsets.keySet shouldBe Set(Simple.name, Typed.name)
          liveFrom(Simple, offsets(Simple.name)) shouldBe Seq(Created("n"))
          liveFrom(Typed, offsets(Typed.name)) shouldBe empty
        case other => fail(s"Expected TagOffsets, got $other")
      }
    }
  }

  private def legacy(id: String, command: Any): Assertion = legacyUnder(id, command)

  private def legacyUnder(persistenceId: String, command: Any): Assertion = {
    val probe  = TestProbe()(system)
    val stream = system.actorOf(Props(new LegacyStream(persistenceId)))
    probe.send(stream, command)
    probe.expectMsg(10.seconds, Done)
    probe.watch(stream)
    system.stop(stream)
    probe.expectTerminated(stream)
    succeed
  }

  private def persistenceIds(): Seq[String] =
    journal.currentPersistenceIds().runWith(Sink.seq).futureValue

  private def events(persistenceId: String): Seq[Any] =
    journal
      .currentEventsByPersistenceId(persistenceId, 0L, Long.MaxValue)
      .map(_.event)
      .runWith(Sink.seq)
      .futureValue

  private def classicItems(id: String): Seq[String] = {
    val aggregate = system.actorOf(Props(classOf[ClassicSimpleAggregate], id))
    try (aggregate ? GetItems(id)).mapTo[Ko].futureValue.failure.asInstanceOf[Items].items
    finally system.stop(aggregate)
  }

  private def typedItems(id: String): List[String] = {
    implicit val scheduler: Scheduler                                   = system.toTyped.scheduler
    implicit val acceptAll: ValidateableCommand[SimpleAggregateCommand] = (cmd: SimpleAggregateCommand) =>
      Future.successful(cmd)
    val gateway = new DefaultTypedCommandGateway[SimpleAggregateCommand](system.toTyped, new SimpleAggregateManager())
    try {
      gateway
        .ask[TypedSimpleAggregate.Response](
          id,
          TypedSimpleAggregate.GetItems(id, AggregateCommandMetaData(OffsetDateTime.now(), None), _)
        )
        .futureValue match {
        case TypedSimpleAggregate.Items(items) => items
        case other                             => fail(s"Unexpected reply $other")
      }
    } finally gateway.shutdown().futureValue
  }

  class TypeView(viewOf: AggregateType) extends AbstractReplayableEventMaterializer(system, withPartialReplay = false) {
    override val logger: Logger         = Logger(LoggerFactory.getLogger(classOf[TypeView]))
    override val aggregateType          = viewOf
    override val matMappingName: String = s"migration-view-$viewOf"
    private val offsets                 = new InMemoryBasedOffsetStore()
    private val handledEvents           = new ConcurrentLinkedQueue[Any]()

    def handled: Seq[Any] = handledEvents.asScala.toList

    override def handleEvent(evt: Any): Future[Done] = {
      handledEvents.add(evt)
      Future.successful(Done)
    }

    override def handleReplayEvent(evt: Any): Future[Done] = handleEvent(evt)

    override def saveOffset(viewIdentifier: String, offset: Offset): Future[Unit] =
      offsets.saveOffset(viewIdentifier, offset)

    override def getOffset(viewIdentifier: String): Future[Offset] = offsets.getOffset(viewIdentifier)
  }
}

class InMemoryAggregateTypeMigrationSpec extends AggregateTypeMigrationSpec {
  override val keepsDeletedEventsInTagQueries: Boolean = true

  override lazy val system: ActorSystem = ActorSystem("InMemoryAggregateTypeMigrationSpec", SpecConfig.testConfig)

  override protected def afterAll(): Unit = TestKit.shutdownActorSystem(system)
}

/** On Postgres in a container; canceled when Docker is not available. */
class R2dbcAggregateTypeMigrationSpec extends AggregateTypeMigrationSpec {
  override val keepsDeletedEventsInTagQueries: Boolean = false

  private var database: Option[R2dbcPostgres]  = None
  private var actorSystem: Option[ActorSystem] = None

  override def available: Boolean = DockerTests.dockerAvailable

  override lazy val system: ActorSystem = {
    val postgres = new R2dbcPostgres()
    database = Some(postgres)
    val created = ActorSystem("R2dbcAggregateTypeMigrationSpec", postgres.config)
    actorSystem = Some(created)
    created
  }

  override protected def afterAll(): Unit = {
    actorSystem.foreach(TestKit.shutdownActorSystem(_))
    database.foreach(_.stop())
  }
}

/** On the JDBC journal with an in-memory H2 database, its tables created by the plugin's `SchemaUtils`. */
class JdbcAggregateTypeMigrationSpec extends AggregateTypeMigrationSpec {
  override val keepsDeletedEventsInTagQueries: Boolean = false

  override lazy val system: ActorSystem = {
    val created = ActorSystem("JdbcAggregateTypeMigrationSpec", JdbcAggregateTypeMigrationSpec.config)
    SchemaUtils.createIfNotExists()(created.toTyped).futureValue
    created
  }

  override protected def afterAll(): Unit = TestKit.shutdownActorSystem(system)
}

object JdbcAggregateTypeMigrationSpec {
  val config: Config = ConfigFactory
    .parseString("""
      pekko.loglevel = INFO
      pekko.persistence.journal.plugin = "jdbc-journal"
      pekko.persistence.snapshot-store.plugin = "jdbc-snapshot-store"
      pekko-persistence-jdbc.shared-databases.slick {
        profile = "slick.jdbc.H2Profile$"
        db {
          url = "jdbc:h2:mem:aggregate-type-migration;DB_CLOSE_DELAY=-1"
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
    """)
    .withFallback(ConfigFactory.load())
}

/**
  * On Cassandra in a container; canceled when Docker is not available. Cassandra keeps deleted events in its tag
  * views, so old streams stay readable by type after `deleteMigrated`.
  */
class CassandraAggregateTypeMigrationSpec extends AggregateTypeMigrationSpec {
  override val keepsDeletedEventsInTagQueries: Boolean = true

  private var cassandra: Option[CassandraContainer] = None
  private var actorSystem: Option[ActorSystem]      = None

  override def available: Boolean = DockerTests.dockerAvailable

  override lazy val system: ActorSystem = {
    val container = new CassandraContainer("cassandra:4.1")
    container.start()
    cassandra = Some(container)
    val created = ActorSystem(
      "CassandraAggregateTypeMigrationSpec",
      CassandraAggregateTypeMigrationSpec.config(container.getContactPoint, container.getLocalDatacenter)
    )
    actorSystem = Some(created)
    created
  }

  override protected def afterAll(): Unit = {
    actorSystem.foreach(TestKit.shutdownActorSystem(_))
    cassandra.foreach(_.stop())
  }
}

object CassandraAggregateTypeMigrationSpec {

  def config(contactPoint: InetSocketAddress, datacenter: String): Config = {
    // Tag queries from NoOffset start at the first bucket; start it shortly before the test instead of in 2015.
    val firstBucket = DateTimeFormatter
      .ofPattern("yyyyMMdd'T'HH:mm")
      .withZone(ZoneOffset.UTC)
      .format(Instant.now().minus(1, ChronoUnit.HOURS).truncatedTo(ChronoUnit.HOURS))
    ConfigFactory
      .parseString(s"""
        pekko.loglevel = INFO
        pekko.persistence.journal.plugin = "pekko.persistence.cassandra.journal"
        pekko.persistence.snapshot-store.plugin = "pekko.persistence.cassandra.snapshot"
        pekko.persistence.cassandra {
          journal.keyspace-autocreate = on
          journal.tables-autocreate = on
          snapshot.keyspace-autocreate = on
          snapshot.tables-autocreate = on
          query.refresh-interval = 200ms
          events-by-tag {
            first-time-bucket = "$firstBucket"
            # one node and one writer: no delayed events to wait for
            eventual-consistency-delay = 200ms
            refresh-interval = 200ms
          }
        }
        datastax-java-driver {
          basic.contact-points = ["${contactPoint.getHostString}:${contactPoint.getPort}"]
          basic.load-balancing-policy.local-datacenter = "$datacenter"
          # creating the keyspaces and tables takes longer than the default 2s
          basic.request.timeout = 30s
          advanced.metadata.schema.request-timeout = 30s
        }
      """)
      .withFallback(ConfigFactory.load())
  }
}
