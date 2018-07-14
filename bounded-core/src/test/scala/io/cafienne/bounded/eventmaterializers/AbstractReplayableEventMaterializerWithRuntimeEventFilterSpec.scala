/*
 * Copyright (C) 2016-2018 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.eventmaterializers

import java.time.ZonedDateTime

import akka.Done
import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.event.{Logging, LoggingAdapter}
import akka.persistence.query.Sequence
import akka.testkit.{TestKit, TestProbe}
import akka.util.Timeout
import com.typesafe.scalalogging.Logger
import io.cafienne.bounded.aggregate.{AggregateRootId, DomainEvent, MetaData}
import io.cafienne.bounded._
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.concurrent.duration._

case class TestedEvent(metaData: MetaData, text: String) extends DomainEvent {
  override def id: AggregateRootId = new AggregateRootId {
    override def idAsString: String = "testaggregate"
  }
}

class AbstractReplayableEventMaterializerWithRuntimeEventFilterSpec
    extends WordSpec
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll {

  //Setup required supporting classes
  implicit val timeout                = Timeout(10.seconds)
  implicit val system                 = ActorSystem("MaterializerTestSystem", SpecConfig.testConfig)
  implicit val logger: LoggingAdapter = Logging(system, getClass)
  implicit val defaultPatience        = PatienceConfig(timeout = Span(4, Seconds), interval = Span(100, Millis))
  implicit val buildInfo              = BuildInfo("spec", "1.0")
  implicit val runtimeInfo            = RuntimeInfo("current")

  val eventStreamListener = TestProbe()

  val currentRuntime = runtimeInfo
  val otherRuntime   = runtimeInfo.copy("previous")

  val currentBuild  = buildInfo
  val previousBuild = buildInfo.copy(version = "0.9")
  val futureBuild   = buildInfo.copy(version = "1.1")

  val currentMeta = MetaData(ZonedDateTime.parse("2018-01-01T17:43:00+01:00"), None, None, currentBuild, currentRuntime)

  val testSet = Seq(
    TestedEvent(currentMeta, "current-current"),
    TestedEvent(currentMeta.copy(buildInfo = previousBuild), "previous-current"),
    TestedEvent(currentMeta.copy(buildInfo = futureBuild), "future-current"),
    TestedEvent(currentMeta.copy(runTimeInfo = otherRuntime), "current-other"),
    TestedEvent(currentMeta.copy(buildInfo = previousBuild, runTimeInfo = otherRuntime), "previous-other"),
    TestedEvent(currentMeta.copy(buildInfo = futureBuild, runTimeInfo = otherRuntime), "future-other")
  )
  //TODO the metadata specifies build - runtime but specifies runtime - build => make this consistent
  "The Event Materializer" must {

    "materialize all given events" in {
      val materializer = new TestMaterializer(DefaultCompatibility)

      val toBeRun = new EventMaterializers(List(materializer))
      whenReady(toBeRun.startUp(false)) { replayResult =>
        logger.debug("replayResult: {}", replayResult)
        assert(replayResult.head.offset == Some(Sequence(6L)))
      }
    }

    "materialize all events within the current runtime and all versions" in {
      val materializer = new TestMaterializer(Compatibility(RuntimeCompatibility.CURRENT, VersionCompatibility.ALL))

      val toBeRun = new EventMaterializers(List(materializer))
      whenReady(toBeRun.startUp(false)) { replayResult =>
        logger.debug("replayResult: {}", replayResult)
        assert(replayResult.head.offset == Some(Sequence(3L)))
      }
    }

    "materialize all events of all runtimes and the current version" in {
      val materializer = new TestMaterializer(Compatibility(RuntimeCompatibility.ALL, VersionCompatibility.CURRENT))

      val toBeRun = new EventMaterializers(List(materializer))
      whenReady(toBeRun.startUp(false)) { replayResult =>
        logger.debug("replayResult: {}", replayResult)
        assert(replayResult.head.offset == Some(Sequence(2L)))
      }
    }

    "materialize all events of the current runtime and the current version" in {
      val materializer = new TestMaterializer(Compatibility(RuntimeCompatibility.CURRENT, VersionCompatibility.CURRENT))

      val toBeRun = new EventMaterializers(List(materializer))
      whenReady(toBeRun.startUp(false)) { replayResult =>
        logger.debug("replayResult: {}", replayResult)
        assert(replayResult.head.offset == Some(Sequence(1L)))
      }
    }

    "materialize all events of the current runtime and till current version" in {
      val materializer =
        new TestMaterializer(Compatibility(RuntimeCompatibility.CURRENT, VersionCompatibility.TILL_DATE))

      val toBeRun = new EventMaterializers(List(materializer))
      whenReady(toBeRun.startUp(false)) { replayResult =>
        logger.debug("replayResult: {}", replayResult)
        assert(replayResult.head.offset == Some(Sequence(2L)))
      }
    }

    "materialize all events of all runtimes and till current version" in {
      val materializer = new TestMaterializer(Compatibility(RuntimeCompatibility.ALL, VersionCompatibility.TILL_DATE))

      val toBeRun = new EventMaterializers(List(materializer))
      whenReady(toBeRun.startUp(false)) { replayResult =>
        logger.debug("replayResult: {}", replayResult)
        assert(replayResult.head.offset == Some(Sequence(4L)))
      }
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

  override def afterAll {
    TestKit.shutdownActorSystem(system, 30.seconds, verifySystemShutdown = true)
  }

  class TestMaterializer(compatible: Compatibility)(implicit buildInfo: BuildInfo, runtimeInfo: RuntimeInfo)
      extends AbstractReplayableEventMaterializer(
        system,
        false,
        new RuntimeAndVersionMaterializerEventFilter(buildInfo, runtimeInfo, compatible)
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
