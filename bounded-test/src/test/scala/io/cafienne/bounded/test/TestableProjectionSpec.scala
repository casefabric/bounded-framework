/*
 * Copyright (C) 2016-2021 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.test

import akka.actor.ActorSystem
import akka.event.{Logging, LoggingAdapter}
import akka.util.Timeout
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpecLike
import akka.Done
import com.typesafe.scalalogging.Logger
import io.cafienne.bounded.eventmaterializers.{AbstractReplayableEventMaterializer, OffsetStoreProvider}
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import DomainProtocol._
import akka.persistence.query.Sequence
import io.cafienne.bounded.aggregate.DomainEvent

import java.time.OffsetDateTime

class TestableProjectionSpec extends AsyncWordSpecLike with Matchers with ScalaFutures with BeforeAndAfterAll {

  implicit val timeout = Timeout(10.seconds)
  implicit val system =
    ActorSystem("TestableProjectionSpecSystem", SpecConfig.testConfig)

  implicit val logger: LoggingAdapter = Logging(system, getClass)

  val testProjectionMaterializer = new TestProjectionMaterializer(system) with OffsetStoreProvider
  val metaDate                   = TestMetaData(OffsetDateTime.now(), None, None)

  "The testable projection" must {

    val testAggregateRootId1 = "arTest1"
    val evt1                 = InitialStateCreated(metaDate, testAggregateRootId1, "initialState")
    val evt2                 = StateUpdated(metaDate, testAggregateRootId1, "updatedState")

    val fixture = TestableProjection.given(Seq(evt1, evt2), Set("ar-test", "aggregate"))

    "Store basic events at the start" in {

      whenReady(fixture.startProjection(testProjectionMaterializer)) { replayResult =>
        logger.info("replayResult: {}", replayResult)
        assert(replayResult.offset == Some(Sequence(2L)))
      }
    }

    "Store more events" in {
      val evt3 = StateUpdated(metaDate, testAggregateRootId1, "morestate")

      fixture.addEvent(evt3)

      testProjectionMaterializer.events.size should be(3)
    }

  }
}

class TestProjectionMaterializer(actorSystem: ActorSystem) extends AbstractReplayableEventMaterializer(actorSystem) {

  /**
    * Tagname used to identify eventstream to listen to
    */
  override val tagName: String = "ar-test"

  /**
    * Mapping name of this listener
    */
  override val matMappingName: String = "test-view"

  override lazy val logger: Logger = Logger(LoggerFactory.getLogger(TestProjectionMaterializer.this.getClass))

  implicit val ec: ExecutionContext = system.dispatcher

  var events: Seq[DomainEvent] = Seq.empty[DomainEvent]

  override def handleReplayEvent(evt: Any): Future[Done] = handleEvent(evt)

  override def handleEvent(evt: Any): Future[Done] = {
    try {
      evt match {
        case event: InitialStateCreated =>
          events = events :+ event
          Future.successful(Done)
        case event: StateUpdated =>
          events = events :+ event
          Future.successful(Done)
        case _ => Future.successful(Done)
      }
    } catch {
      case ex: Throwable =>
        logger.error(
          "Unable to process event: " + evt.getClass.getSimpleName + Option(ex.getCause)
            .map(ex => ex.getMessage)
            .getOrElse("") + s" ${ex.getMessage} " + " exception: " + logException(ex),
          ex
        )
        Future.failed(ex)
    }
  }
}
