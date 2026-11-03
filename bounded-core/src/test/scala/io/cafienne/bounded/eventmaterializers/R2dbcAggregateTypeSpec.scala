/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.eventmaterializers

import org.apache.pekko.Done
import org.apache.pekko.actor.{ActorSystem, Props}
import org.apache.pekko.actor.typed.Scheduler
import org.apache.pekko.actor.typed.scaladsl.adapter._
import org.apache.pekko.persistence.Persistence
import org.apache.pekko.persistence.query.{NoOffset, TimestampOffset}
import org.apache.pekko.testkit.{TestKit, TestProbe}
import org.apache.pekko.util.Timeout
import com.typesafe.scalalogging.Logger
import io.cafienne.bounded.R2dbcPostgres
import io.cafienne.bounded.aggregate.{
  AggregateRootActorSpec,
  AggregateType,
  ClassicSimpleAggregate,
  DomainCommand,
  Ok,
  ValidateableCommand
}
import io.cafienne.bounded.aggregate.ClassicSimpleAggregate._
import io.cafienne.bounded.aggregate.TypedSimpleAggregate
import io.cafienne.bounded.aggregate.TypedSimpleAggregate.{AggregateCommandMetaData, SimpleAggregateCommand}
import io.cafienne.bounded.aggregate.SimpleAggregateManager
import io.cafienne.bounded.aggregate.typed.DefaultTypedCommandGateway
import io.cafienne.bounded.eventmaterializers.offsetstores.DockerTests
import io.cafienne.bounded.pekko.persistence.{R2dbcAggregateTypeReadJournal, ReadJournalProvider}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.LoggerFactory

import java.time.OffsetDateTime
import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.immutable.Seq
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

/**
  * Event materializers and combined logic on the R2DBC journal (Postgres in a container), reading aggregate types by entity
  * type. Canceled when Docker is not available.
  */
class R2dbcAggregateTypeSpec
    extends AnyWordSpec
    with Matchers
    with ScalaFutures
    with Eventually
    with BeforeAndAfterAll {
  import DockerTests.dockerAvailable

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(30, Seconds), interval = Span(100, Millis))

  private var database: Option[R2dbcPostgres]  = None
  private var actorSystem: Option[ActorSystem] = None

  // Started by the first test that runs, so nothing starts when Docker is not available.
  private lazy val system: ActorSystem = {
    val postgres = new R2dbcPostgres()
    database = Some(postgres)
    val created = ActorSystem("R2dbcAggregateTypeSpec", postgres.config)
    actorSystem = Some(created)
    created
  }

  private def db: R2dbcPostgres = { system; database.get }

  private val Simple = ClassicSimpleAggregate.Type
  private val Order  = AggregateType("order")
  private val Typed  = AggregateType(TypedSimpleAggregate.aggregateRootTag)

  "The read journal provider on the R2DBC journal" should {

    "read aggregate types by entity type and refuse queries by tag" in {
      assume(dockerAvailable, "Docker is not available")
      val provider = new ReadJournalProvider with io.cafienne.bounded.pekko.ActorSystemProvider {
        override implicit def system: ActorSystem = R2dbcAggregateTypeSpec.this.system
      }

      provider.aggregateTypeJournal shouldBe an[R2dbcAggregateTypeReadJournal]
      val error = the[UnsupportedOperationException] thrownBy provider.readJournal
      error.getMessage should include("aggregateTypeJournal")
    }
  }

  "Aggregates on the R2DBC journal" should {

    "store their events under type|id with the type as entity type; classic events without extra tags untagged" in {
      assume(dockerAvailable, "Docker is not available")
      val id = "stored-shared"
      send(Props(classOf[ClassicSimpleAggregate], id), Create(id), AddItem(id, "simple item"))
      send(Props(classOf[AggregateRootActorSpec.OrderAggregate], id), Create(id))
      typedCreate(id)

      db.query(
        "SELECT persistence_id, entity_type, tags FROM event_journal WHERE persistence_id LIKE ? ORDER BY persistence_id, seq_nr",
        s"%|$id"
      )(row =>
        (
          row.getString(1),
          row.getString(2),
          Option(row.getArray(3)).fold(List.empty[String])(_.getArray.asInstanceOf[Array[String]].toList)
        )
      ) shouldBe List(
        (s"${Typed.name}|$id", Typed.name, List(Typed.name)),
        (s"order|$id", "order", Nil),
        (s"simple|$id", "simple", Nil),
        (s"simple|$id", "simple", Nil)
      )
    }
  }

  "A materializer on the R2DBC journal" should {

    "replay only the events of its aggregate type and resume from the stored offset" in {
      assume(dockerAvailable, "Docker is not available")
      val id = "replay"
      send(Props(classOf[ClassicSimpleAggregate], id), Create(id), AddItem(id, "replay 1"))
      send(Props(classOf[AggregateRootActorSpec.OrderAggregate], id), Create(id), AddItem(id, "not for simple"))
      typedCreate(id)

      val first  = new TypeView("replay-view", Simple)
      val offset = first.replayEvents().futureValue
      offset shouldBe a[TimestampOffset]
      first.handled should contain allOf (Created(id), ItemAdded(id, "replay 1"))
      first.handled should not contain ItemAdded(id, "not for simple")
      first.handled.collect { case e: TypedSimpleAggregate.Created => e } shouldBe empty
      first.getOffset(first.viewIdentifier).futureValue shouldBe offset

      send(Props(classOf[ClassicSimpleAggregate], id), AddItem(id, "replay 2"))
      val second = new TypeView("replay-view", Simple)
      second.replayEvents().futureValue
      second.handled shouldBe Seq(ItemAdded(id, "replay 2"))
    }

    "read the events of typed aggregates by their type" in {
      assume(dockerAvailable, "Docker is not available")
      val id = "typed-view"
      typedCreate(id)

      val view = new TypeView("typed-view", Typed, keepOffset = false)
      view.replayEvents().futureValue
      view.handled.collect { case e: TypedSimpleAggregate.Created => e.id } should contain(id)
      view.handled.collect { case e: ItemAdded => e } shouldBe empty
    }

    "listen to new events, and deliver an event committed late exactly once" in {
      assume(dockerAvailable, "Docker is not available")
      val source = "late-source"
      send(Props(classOf[AggregateRootActorSpec.OrderAggregate], source), Create(source), AddItem(source, "late"))
      val view                = new TypeView("late-view", Simple, keepOffset = false)
      val (killSwitch, ended) = view.registerListenerWithKillSwitch(Some(NoOffset))

      val id = "live"
      send(Props(classOf[ClassicSimpleAggregate], id), Create(id))
      eventually(view.handled should contain(Created(id)))

      // a row of an earlier transaction that becomes visible after the view has read past its timestamp
      copyEvent(s"order|$source", 2L, s"simple|late-copy", 1L, millisAgo = 3000)

      eventually(view.handled should contain(ItemAdded(source, "late")))
      Thread.sleep(3000) // backtracking reads its window again
      view.handled.count(_ == ItemAdded(source, "late")) shouldBe 1
      view.handled.count(_ == Created(id)) shouldBe 1

      killSwitch.shutdown()
      ended.futureValue shouldBe a[TimestampOffset]
    }

    "deliver the events of one aggregate in order when a later one is read first" in {
      assume(dockerAvailable, "Docker is not available")
      val source = "gap-source"
      send(
        Props(classOf[AggregateRootActorSpec.OrderAggregate], source),
        Create(source),
        AddItems(source, Seq("gap 1", "gap 2", "gap 3"))
      )
      val view            = new TypeView("gap-view", Simple, keepOffset = false)
      val (killSwitch, _) = view.registerListenerWithKillSwitch(Some(NoOffset))

      copyEvent(s"order|$source", 2L, "simple|gap", 1L, millisAgo = 0)
      eventually(view.handled should contain(ItemAdded(source, "gap 1")))
      // event 2 is committed with an older timestamp than event 3, so the query reads 3 first
      copyEvent(s"order|$source", 3L, "simple|gap", 2L, millisAgo = 3000)
      copyEvent(s"order|$source", 4L, "simple|gap", 3L, millisAgo = 0)

      eventually(view.handled.collect { case ItemAdded(`source`, item) =>
        item
      } shouldBe Seq("gap 1", "gap 2", "gap 3"))
      Thread.sleep(3000) // backtracking delivers event 2 again
      view.handled.collect { case ItemAdded(`source`, item) => item } shouldBe Seq("gap 1", "gap 2", "gap 3")
      killSwitch.shutdown()
    }
  }

  "A combined logic on the R2DBC journal" should {

    "read two aggregate types and resume per type from the stored offsets" in {
      assume(dockerAvailable, "Docker is not available")
      val id = "combined-logic"
      send(Props(classOf[ClassicSimpleAggregate], id), Create(id))
      send(Props(classOf[AggregateRootActorSpec.OrderAggregate], id), Create(id))

      val first           = new TypeCombinedLogic("r2dbc-combined-logic", List(Simple, Order))
      val (killSwitch, _) = first.registerListenerWithKillSwitch(None)
      eventually(first.handled.count(_ == Created(id)) shouldBe 2)
      // stored up to and including the last event of each type
      eventually {
        first.getOffset(first.viewIdentifier).futureValue match {
          case TagOffsets(offsets) =>
            offsets.keySet shouldBe Set(Simple.name, Order.name)
            offsets(Simple.name).asInstanceOf[TimestampOffset].seen.keySet should contain(s"simple|$id")
            offsets(Order.name).asInstanceOf[TimestampOffset].seen.keySet should contain(s"order|$id")
          case other => fail(s"Expected TagOffsets, got $other")
        }
      }
      killSwitch.shutdown()

      send(Props(classOf[ClassicSimpleAggregate], id), AddItem(id, "combined logic simple"))
      send(Props(classOf[AggregateRootActorSpec.OrderAggregate], id), AddItem(id, "combined logic order"))
      val second                = new TypeCombinedLogic("r2dbc-combined-logic", List(Simple, Order))
      val (secondKillSwitch, _) = second.registerListenerWithKillSwitch(None)
      eventually(
        second.handled should contain allOf (
          ItemAdded(id, "combined logic simple"),
          ItemAdded(id, "combined logic order")
        )
      )
      Thread.sleep(3000) // backtracking reads its window again
      second.handled.count(_ == ItemAdded(id, "combined logic simple")) shouldBe 1
      second.handled.count(_ == ItemAdded(id, "combined logic order")) shouldBe 1
      // The events at the stored offsets are not handled again. Older ones within the backtracking window before them
      // can be: after a restart, delivery is at-least-once.
      second.handled should not contain Created(id)
      secondKillSwitch.shutdown()
    }
  }

  // Sends the commands to a new instance of the aggregate, one at a time, and stops it.
  private def send(props: Props, commands: DomainCommand*): Unit = {
    val probe     = TestProbe()(system)
    val aggregate = system.actorOf(props)
    commands.foreach { command =>
      probe.send(aggregate, command)
      probe.expectMsgType[Ok](10.seconds)
    }
    probe.watch(aggregate)
    system.stop(aggregate)
    probe.expectTerminated(aggregate)
    ()
  }

  private def typedCreate(id: String): Unit = {
    implicit val timeout: Timeout                                       = Timeout(10.seconds)
    implicit val ec: ExecutionContext                                   = system.dispatcher
    implicit val scheduler: Scheduler                                   = system.toTyped.scheduler
    implicit val acceptAll: ValidateableCommand[SimpleAggregateCommand] = (cmd: SimpleAggregateCommand) =>
      Future.successful(cmd)
    val gateway = new DefaultTypedCommandGateway[SimpleAggregateCommand](system.toTyped, new SimpleAggregateManager())
    gateway
      .ask[TypedSimpleAggregate.Response](
        id,
        TypedSimpleAggregate.Create(id, AggregateCommandMetaData(OffsetDateTime.now(), None), _)
      )
      .futureValue shouldBe TypedSimpleAggregate.OK
    gateway.shutdown().futureValue
  }

  // Inserts a copy of a stored event under another persistence id, with a db_timestamp the given time in the past.
  private def copyEvent(fromPid: String, fromSeqNr: Long, toPid: String, toSeqNr: Long, millisAgo: Long): Unit = {
    val entityType = AggregateType.of(toPid).get.name
    val inserted   = db.update(
      """INSERT INTO event_journal (slice, entity_type, persistence_id, seq_nr, db_timestamp, event_ser_id,
        |  event_ser_manifest, event_payload, deleted, writer, adapter_manifest, tags)
        |SELECT ?, ?, ?, ?, now() - (? * interval '1 millisecond'), event_ser_id, event_ser_manifest, event_payload,
        |  false, writer, adapter_manifest, ARRAY[?]
        |FROM event_journal WHERE persistence_id = ? AND seq_nr = ?""".stripMargin,
      Persistence(system).sliceForPersistenceId(toPid),
      entityType,
      toPid,
      toSeqNr,
      millisAgo,
      entityType,
      fromPid,
      fromSeqNr
    )
    inserted shouldBe 1
    ()
  }

  class TypeView(name: String, viewOf: AggregateType, keepOffset: Boolean = true)
      extends AbstractReplayableEventMaterializer(system, keepOffset)
      with ReadJournalOffsetStore {
    override val logger: Logger         = Logger(LoggerFactory.getLogger(classOf[TypeView]))
    override val aggregateType          = viewOf
    override val matMappingName: String = name
    private val handledEvents           = new ConcurrentLinkedQueue[Any]()

    def handled: Seq[Any] = handledEvents.asScala.toList

    override def handleEvent(evt: Any): Future[Done] = {
      handledEvents.add(evt)
      Future.successful(Done)
    }

    override def handleReplayEvent(evt: Any): Future[Done] = handleEvent(evt)
  }

  class TypeCombinedLogic(name: String, types: List[AggregateType])
      extends CombinedLogicEventMaterializer(system)
      with ReadJournalOffsetStore {
    override val logger: Logger         = Logger(LoggerFactory.getLogger(classOf[TypeCombinedLogic]))
    override val aggregateTypes         = types
    override val matMappingName: String = name
    private val handledEvents           = new ConcurrentLinkedQueue[Any]()

    def handled: Seq[Any] = handledEvents.asScala.toList

    override def handleEvent(evt: Any): Future[Done] = {
      handledEvents.add(evt)
      Future.successful(Done)
    }
  }

  override protected def afterAll(): Unit = {
    actorSystem.foreach(TestKit.shutdownActorSystem(_))
    database.foreach(_.stop())
  }
}
