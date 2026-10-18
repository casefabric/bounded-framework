/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.eventmaterializers

import java.time.OffsetDateTime
import org.apache.pekko.Done
import org.apache.pekko.actor.{ActorSystem, ExtendedActorSystem, PoisonPill, Props}
import org.apache.pekko.persistence.query.Sequence
import org.apache.pekko.testkit.TestProbe
import com.typesafe.scalalogging.Logger
import io.cafienne.bounded.aggregate.DomainEvent
import org.apache.pekko.actor.testkit.typed.scaladsl.{LogCapturing, ScalaTestWithActorTestKit}
import org.apache.pekko.projection.ProjectionId
import org.apache.pekko.projection.testkit.scaladsl.{ProjectionTestKit, TestProjection, TestSourceProvider}
import org.apache.pekko.stream.scaladsl.Source
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.LoggerFactory
import org.apache.pekko.projection.scaladsl.Handler

import scala.concurrent.Future

case class TestMetaData(
  timestamp: OffsetDateTime,
  userContext: Option[String]
)

case class TestedEvent(metaData: TestMetaData, text: String) extends DomainEvent {
  def id: String = "entityId"
}

class UserEventFilter(
  userToFilter: String
) extends MaterializerEventFilter {

  override def filter(evt: DomainEvent): Boolean = {
    evt match {
      case TestedEvent(metaData, text)
          if (metaData.userContext.isDefined && metaData.userContext.get.equalsIgnoreCase("user-b")) =>
        true
      case _ => false
    }
  }

}

class AbstractReplayableEventMaterializerWithEventFilterSpec
    extends ScalaTestWithActorTestKit(SpecConfig.testConfig)
    with AnyWordSpecLike
    with Matchers
    with ScalaFutures
    with LogCapturing {

  //Setup required supporting classes

//  implicit val system: ActorSystem =
//    ActorSystem("MaterializerTestSystem", PersistenceTestKitPlugin.config.withFallback(SpecConfig.testConfig))
  // implicit val timeout: Timeout       = Timeout(10.seconds)
  // implicit val logger: LoggingAdapter = Logging(system, getClass)
//  implicit val defaultPatience: PatienceConfig =
//    PatienceConfig(timeout = Span(4, Seconds), interval = Span(100, Millis))

  //val testKit = PersistenceTestKit(system)
  private val projectionTestKit           = ProjectionTestKit(system)
  val projectionId: ProjectionId          = ProjectionId("name", "key")
  implicit val classicSystem: ActorSystem = system.classicSystem
  val eventStreamListener                 = TestProbe()

  val currentMeta =
    TestMetaData(OffsetDateTime.parse("2018-01-01T17:43:00+01:00"), None)

  val testSet = Seq(
    TestedEvent(currentMeta, "current-current"),
    TestedEvent(currentMeta.copy(userContext = Some("user-a")), "current+1"),
    TestedEvent(currentMeta.copy(userContext = Some("user-b")), "current+2"),
    TestedEvent(currentMeta.copy(userContext = Some("user-a")), "current+3"),
    TestedEvent(currentMeta.copy(userContext = Some("user-b")), "current+4"),
    TestedEvent(currentMeta.copy(userContext = Some("user-b")), "current+5")
  )

  def handler(strBuffer: StringBuffer, predicate: Int => Boolean): Handler[Int] = new Handler[Int] {
    override def process(env: Int): Future[Done] = {
      if (predicate(env)) concat(env)
      Future.successful(Done)
    }

    def concat(i: Int) = {
      if (strBuffer.toString.isEmpty) strBuffer.append(i)
      else strBuffer.append("-").append(i)
    }
  }

  "The Event Materializer" must {

    "run an function handler" in {
      val strBuffer = new StringBuffer()
      val sp        = TestSourceProvider(Source(1 to 6), (i: Int) => i)
      val prj       = TestProjection(projectionId, sp, () => handler(strBuffer, _ <= 6))

      // stop as soon we observe that all expected elements passed through
      projectionTestKit.run(prj) {
        strBuffer.toString shouldBe "1-2-3-4-5-6"
      }
    }

    "materialize all given events" in {
      val materializer = new TestMaterializer()

      val toBeRun = new EventMaterializers(List(materializer))
      whenReady(toBeRun.startUp(false)) { replayResult =>
        system.log.debug("replayResult: {}", replayResult)
        assert(replayResult.head.offset == Some(Sequence(6L)))
      }
      system.log.debug("DUMP all given events {}", materializer.storedEvents)
      assert(materializer.storedEvents.size == 6)
    }

    "materialize all events for user-b" in {
      val materializer = new TestMaterializer(new UserEventFilter("user-b"))

      val toBeRun = new EventMaterializers(List(materializer))
      whenReady(toBeRun.startUp(false)) { replayResult =>
        system.log.debug("replayResult: {}", replayResult)
        assert(replayResult.head.offset == Some(Sequence(6L)))
      }
      system.log.debug("DUMP current runtime and all versions {}", materializer.storedEvents)
      assert(materializer.storedEvents.size == 3)
    }

  }

  private def populateEventStore(evt: Seq[DomainEvent]): Unit = {
    val storeEventsActor =
      // the typed testkit system has a custom user guardian, so a classic actor can only be created as system actor
      classicSystem
        .asInstanceOf[ExtendedActorSystem]
        .systemActorOf(Props(classOf[CreateEventsInStoreActor], evt.head.id), "create-events-actor")

    val testProbe = TestProbe()
    testProbe watch storeEventsActor

    classicSystem.eventStream.subscribe(eventStreamListener.ref, classOf[EventProcessed])

    evt foreach { event =>
      testProbe.send(storeEventsActor, event)
      testProbe.expectMsgAllConformingOf(classOf[DomainEvent])
    }

    storeEventsActor ! PoisonPill
    val terminated = testProbe.expectTerminated(storeEventsActor)
    assert(terminated.existenceConfirmed)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    populateEventStore(testSet)
  }

  class TestMaterializer(eventFilter: MaterializerEventFilter = NoFilterEventFilter)
      extends AbstractReplayableEventMaterializer(
        classicSystem,
        false,
        eventFilter
      ) {

    var storedEvents = Seq[DomainEvent]()

    override val logger: Logger = Logger(LoggerFactory.getLogger(TestMaterializer.this.getClass))

    /**
      * Tagname used to identify eventstream to listen to
      */
    override val tagName: String = "testar"

    /**
      * Mapping name of this listener
      */
    override val matMappingName: String = "testar"

    /**
      * Handle new incoming event
      *
      * @param evt event
      */
    override def handleEvent(evt: Any): Future[Done] = {
      logger.debug("TestMaterializer got event {} ", evt)
      evt match {
        case x: DomainEvent => storedEvents = storedEvents :+ x
        case other          => logger.warn("unkown event will not be stored {}", other)
      }
      Future.successful(Done)
    }

    override def handleReplayEvent(evt: Any): Future[Done] = handleEvent(evt)

    override def toString: String = s"TestMaterializer $tagName contains ${storedEvents.mkString(",")}"
  }

}
