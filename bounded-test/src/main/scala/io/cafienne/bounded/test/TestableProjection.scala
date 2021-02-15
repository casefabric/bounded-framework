/*
 * Copyright (C) 2016-2021 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.test

import java.util.UUID
import akka.actor._
import akka.persistence.inmemory.extension.{InMemoryJournalStorage, InMemorySnapshotStorage, StorageExtension}
import akka.testkit.TestProbe
import akka.util.Timeout
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

  def given(
    evt: Seq[DomainEvent],
    tags: Set[String] = Set.empty
  )(implicit system: ActorSystem, timeout: Timeout = 2.seconds): TestableProjection = {
    //Cleanup the store before this test is ran.
    val tp = TestProbe()
    tp.send(StorageExtension(system).journalStorage, InMemoryJournalStorage.ClearJournal)
    tp.expectMsg(akka.actor.Status.Success(""))
    tp.send(StorageExtension(system).snapshotStorage, InMemorySnapshotStorage.ClearSnapshots)
    tp.expectMsg(akka.actor.Status.Success(""))

    val testedProjection = new TestableProjection(system, timeout, tags)
    OffsetStoreProvider.getInMemoryStore().clear()
    testedProjection.storeEvents(evt)
    testedProjection
  }

}

class TestableProjection private (system: ActorSystem, timeout: Timeout, tags: Set[String]) {

  private var eventMaterializers: Option[EventMaterializers] = _
  private implicit val executionContext: ExecutionContext    = system.dispatcher
  private implicit val actorSystem: ActorSystem              = system
  private var materializerId: Option[UUID]                   = None

  val eventStreamListener = TestProbe()

  if (!system.settings.config.hasPath("bounded.eventmaterializers.publish") || !system.settings.config.getBoolean(
        "bounded.eventmaterializers.publish"
      )) {
    system.log.error("Config property bounded.eventmaterializers.publish must be enabled")
  }

  def startProjection(projector: AbstractEventMaterializer): Future[EventMaterializers.ReplayResult] = {
    materializerId = Some(projector.materializerId)
    eventMaterializers = Some(new EventMaterializers(List(projector)))
    eventMaterializers.get.startUp(true).map(list => list.head)
  }

  def addEvents(evt: Seq[DomainEvent]): Unit = {
    eventMaterializers.fold(throw new IllegalStateException("You start the projection before you add events"))(_ => {
      storeEvents(evt)
    })
  }

  def addEvent(evt: DomainEvent): Unit = {
    eventMaterializers.fold(throw new IllegalStateException("You start the projection before you add events"))(_ => {
      storeEvents(Seq(evt))
    })
  }

  // Blocking way to store events.
  private def storeEvents(evt: Seq[DomainEvent]): Unit = {
    val storeEventsActor =
      system.actorOf(Props(classOf[CreateEventsInStoreActor], evt.head.id, tags), "create-events-actor")

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

  private def waitTillLastEventIsProcessed(evt: Seq[DomainEvent]) = {
    if (materializerId.isDefined) {
      eventStreamListener.fishForSpecificMessage(timeout.duration, "wait till last event is processed") {
        case e: EventProcessed if materializerId.get == e.materializerId && evt.last == e.evt =>
          system.log.debug("catch " + e)
      }
    }
  }
}
