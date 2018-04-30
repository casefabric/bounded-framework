/*
 * Copyright (C) 2016-2018 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.test

import java.util.UUID

import akka.actor._
import akka.persistence.inmemory.extension.{InMemoryJournalStorage, InMemorySnapshotStorage, StorageExtension}
import akka.testkit.TestProbe
import akka.util.Timeout
import io.cafienne.bounded.aggregate._
import io.cafienne.bounded.akka.persistence.eventmaterializers.{
  AbstractEventMaterializer,
  EventMaterializers,
  EventProcessed
}
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

object TestableProjection {

  def given(evt: Seq[DomainEvent])(implicit system: ActorSystem, timeout: Timeout): TestableProjection = {
    //Cleanup the store before this test is ran.
    val tp = TestProbe()
    tp.send(StorageExtension(system).journalStorage, InMemoryJournalStorage.ClearJournal)
    tp.expectMsg(akka.actor.Status.Success(""))
    tp.send(StorageExtension(system).snapshotStorage, InMemorySnapshotStorage.ClearSnapshots)
    tp.expectMsg(akka.actor.Status.Success(""))

    val testedProjection = new TestableProjection(system, timeout)
    testedProjection.storeEvents(evt)
    testedProjection
  }

}

class TestableProjection private (system: ActorSystem, timeout: Timeout) {

  private var eventMaterializers: Option[EventMaterializers] = _
  private implicit val executionContext: ExecutionContext    = system.dispatcher
  private implicit val actorSystem: ActorSystem              = system
  private var materializerId: Option[UUID]                   = None

  val eventStreamListener = TestProbe()

  if (!system.settings.config.hasPath("io.bounded.eventmaterializers.publish") || !system.settings.config.getBoolean(
        "io.bounded.eventmaterializers.publish"
      )) {
    system.log.error("Config property io.bounded.eventmaterializers.publish must be enabled")
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

  // Blocking way to store events.
  private def storeEvents(evt: Seq[DomainEvent]): Unit = {
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

    waitTillLastEventIsProcessed(evt)
  }

  private def waitTillLastEventIsProcessed(evt: Seq[DomainEvent]) = {
    if (materializerId.isDefined) {
      eventStreamListener.fishForSpecificMessage(2.seconds, "wait till last event is processed") {
        case e: EventProcessed if materializerId.get == e.materializerId && evt.last == e.evt => ()
      }
    }
  }
}
