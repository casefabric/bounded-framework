/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.test

import org.apache.pekko.Done
import scala.compiletime.uninitialized

import java.util.UUID
import org.apache.pekko.actor._
import org.apache.pekko.testkit.TestProbe
import org.apache.pekko.util.Timeout
import io.cafienne.bounded.aggregate._
import io.cafienne.bounded.eventmaterializers.{
  AbstractEventMaterializer,
  EventMaterializers,
  EventProcessed,
  OffsetStoreProvider
}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

object TestableProjection {

  /**
    * Stores the events as an aggregate of the given type would (persistence id `aggregateType|<id of the first
    * event>`, tagged with the type), before the projection starts.
    */
  def givenEvents(
    evt: Seq[DomainEvent],
    aggregateType: AggregateType
  )(implicit system: ActorSystem, timeout: Timeout = 2.seconds): TestableProjection = {
    val testedProjection = new TestableProjection(system, timeout, aggregateType)
    OffsetStoreProvider.getInMemoryStore().clear()
    testedProjection.storeEvents(evt)
    testedProjection
  }

}

class TestableProjection private (system: ActorSystem, timeout: Timeout, aggregateType: AggregateType) {

  private var eventMaterializers: Option[EventMaterializers] = uninitialized
  private implicit val executionContext: ExecutionContext    = system.dispatcher
  private implicit val actorSystem: ActorSystem              = system
  private var materializerId: Option[UUID]                   = None

  val eventStreamListener = TestProbe()

  if (
    !system.settings.config.hasPath("bounded.eventmaterializers.publish") || !system.settings.config.getBoolean(
      "bounded.eventmaterializers.publish"
    )
  ) {
    system.log.error("Config property bounded.eventmaterializers.publish must be enabled")
  }

  def startProjection(projector: AbstractEventMaterializer): Future[EventMaterializers.ReplayResult] = {
    materializerId = Some(projector.materializerId)
    eventMaterializers = Some(new EventMaterializers(List(projector)))
    eventMaterializers.get.startUp(true).map(list => list.head)
  }

  def addEvents(evt: Seq[DomainEvent]): Future[Done] = {
    eventMaterializers.fold(throw new IllegalStateException("You start the projection before you add events"))(_ => {
      storeEvents(evt)
    })
  }

  def addEvent(evt: DomainEvent): Future[Done] = {
    eventMaterializers.fold(throw new IllegalStateException("You start the projection before you add events"))(_ => {
      storeEvents(Seq(evt))
    })
  }

  // Blocking way to store events.
  private def storeEvents(evt: Seq[DomainEvent]): Future[Done] = {

    val storeEventsActor =
      system.actorOf(
        Props(
          classOf[CreateEventsInStoreActor],
          aggregateType.persistenceId(evt.head.id).id,
          Set(aggregateType.name)
        ),
        "create-events-actor"
      )

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

    waitTillLastEventIsProcessed(evt)
  }

  private def waitTillLastEventIsProcessed(evt: Seq[DomainEvent]): Future[Done] = {
    if (materializerId.isDefined) {
      var messageProcessedCounter = 0
      while (messageProcessedCounter < evt.length) {
        eventStreamListener.fishForSpecificMessage(timeout.duration, "wait till last event is processed") {
          case e: EventProcessed if materializerId.get == e.materializerId =>
            messageProcessedCounter += 1
            system.log.debug("catch " + e)
          case other => system.log.debug("other message fished: {}", other)
        }
      }
      Future.successful(Done)
    } else {
      Future.failed(new Exception("No materializer with id:" + materializerId + " found"))
    }
  }
}
