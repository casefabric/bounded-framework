/*
 * Copyright (C) 2016-2023 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.eventmaterializers

import java.time.OffsetDateTime
import akka.Done
import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.event.{Logging, LoggingAdapter}
import akka.persistence.query.Sequence
import akka.testkit.{TestKit, TestProbe}
import akka.util.Timeout
import com.typesafe.scalalogging.Logger
import io.cafienne.bounded.aggregate.DomainEvent
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.concurrent.duration._

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
    extends AnyWordSpec
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll {

  //Setup required supporting classes
  implicit val timeout: Timeout       = Timeout(10.seconds)
  implicit val system: ActorSystem    = ActorSystem("MaterializerTestSystem", SpecConfig.testConfig)
  implicit val logger: LoggingAdapter = Logging(system, getClass)
  implicit val defaultPatience: PatienceConfig =
    PatienceConfig(timeout = Span(4, Seconds), interval = Span(100, Millis))

  val eventStreamListener = TestProbe()

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

  "The Event Materializer" must {

    "materialize all given events" in {
      val materializer = new TestMaterializer()

      val toBeRun = new EventMaterializers(List(materializer))
      whenReady(toBeRun.startUp(false)) { replayResult =>
        logger.debug("replayResult: {}", replayResult)
        assert(replayResult.head.offset == Some(Sequence(6L)))
      }
      logger.debug("DUMP all given events {}", materializer.storedEvents)
      assert(materializer.storedEvents.size == 6)
    }

    "materialize all events for user-b" in {
      val materializer = new TestMaterializer(new UserEventFilter("user-b"))

      val toBeRun = new EventMaterializers(List(materializer))
      whenReady(toBeRun.startUp(false)) { replayResult =>
        logger.debug("replayResult: {}", replayResult)
        assert(replayResult.head.offset == Some(Sequence(6L)))
      }
      logger.debug("DUMP current runtime and all versions {}", materializer.storedEvents)
      assert(materializer.storedEvents.size == 3)
    }

  }

  private def populateEventStore(evt: Seq[DomainEvent]): Unit = {
    val storeEventsActor = system.actorOf(Props(classOf[CreateEventsInStoreActor], evt.head.id), "create-events-actor")

    val testProbe = TestProbe()
    testProbe watch storeEventsActor

    system.eventStream.subscribe(eventStreamListener.ref, classOf[EventProcessed])

    evt foreach { event =>
      testProbe.send(storeEventsActor, event)
      testProbe.expectMsgAllConformingOf(classOf[DomainEvent])
    }

    storeEventsActor ! PoisonPill
    val terminated = testProbe.expectTerminated(storeEventsActor)
    assert(terminated.existenceConfirmed)

  }

  override def beforeAll(): Unit = {
    populateEventStore(testSet)
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system, 30.seconds, verifySystemShutdown = true)
  }

  class TestMaterializer(eventFilter: MaterializerEventFilter = NoFilterEventFilter)
      extends AbstractReplayableEventMaterializer(
        system,
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
