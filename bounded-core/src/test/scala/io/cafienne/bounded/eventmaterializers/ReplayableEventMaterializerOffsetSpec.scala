/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.eventmaterializers

import org.apache.pekko.Done
import org.apache.pekko.actor.{ActorSystem, ExtendedActorSystem, PoisonPill, Props}
import org.apache.pekko.actor.testkit.typed.scaladsl.{LogCapturing, ScalaTestWithActorTestKit}
import org.apache.pekko.persistence.journal.Tagged
import org.apache.pekko.persistence.query.{NoOffset, Offset, Sequence}
import org.apache.pekko.testkit.TestProbe
import com.typesafe.scalalogging.Logger
import io.cafienne.bounded.aggregate.DomainEvent
import io.cafienne.bounded.eventmaterializers.offsetstores.InMemoryBasedOffsetStore
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.LoggerFactory

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.Future
import scala.jdk.CollectionConverters._

case class ReplayTestEvent(id: String, text: String) extends DomainEvent

/**
  * Characterizes replay with offset saving and resuming, followed by live listening, for a plain
  * (single tag) replayable materializer.
  */
class ReplayableEventMaterializerOffsetSpec
    extends ScalaTestWithActorTestKit(SpecConfig.testConfig)
    with AnyWordSpecLike
    with Matchers
    with LogCapturing {

  implicit val classicSystem: ActorSystem = system.classicSystem

  val tag            = "replay-offset-tag"
  val persistenceId  = "replay-offset-ar"
  val initialEvents  = (1 to 4).map(i => ReplayTestEvent(persistenceId, s"e$i"))
  val sharedOffsets  = new InMemoryBasedOffsetStore()
  val viewId         = "replay-offset-view"

  "A replayable event materializer with partial replay" must {

    "replay all events and save the offset of the last one" in {
      val materializer = new TestReplayable(sharedOffsets)

      materializer.replayEvents().futureValue shouldBe Sequence(4L)

      materializer.replayed shouldBe initialEvents
      eventually(sharedOffsets.getOffset(viewId).futureValue shouldBe Sequence(4L))
    }

    "resume replay from the saved offset" in {
      val materializer = new TestReplayable(sharedOffsets)

      materializer.replayEvents().futureValue shouldBe Sequence(4L)

      materializer.replayed shouldBe empty
    }

    "listen from the replayed offset and save the offset of live events" in {
      val materializer = new TestReplayable(sharedOffsets)
      val listener     = TestProbe()
      classicSystem.eventStream.subscribe(listener.ref, classOf[EventProcessed])
      val replayed = new EventMaterializers(List(materializer)).startUp(true).futureValue
      replayed.head.offset shouldBe Some(Sequence(4L))

      storeEvents(Seq(ReplayTestEvent(persistenceId, "e5")))

      listener.fishForMessage() {
        case e: EventProcessed => e.materializerId == materializer.materializerId
      }
      materializer.live shouldBe Seq(ReplayTestEvent(persistenceId, "e5"))
      eventually(sharedOffsets.getOffset(viewId).futureValue shouldBe Sequence(5L))
    }
  }

  "A replayable event materializer without partial replay" must {

    "replay from the start and not save offsets" in {
      val offsets      = new InMemoryBasedOffsetStore()
      val materializer = new TestReplayable(offsets, withPartialReplay = false)

      materializer.replayEvents().futureValue shouldBe Sequence(5L)

      materializer.replayed should have size 5
      offsets.getOffset(viewId).futureValue shouldBe NoOffset
    }
  }

  private val storeCounter = new AtomicInteger()

  private def storeEvents(events: Seq[DomainEvent]): Unit = {
    val storeEventsActor = classicSystem
      .asInstanceOf[ExtendedActorSystem]
      .systemActorOf(Props(classOf[CreateEventsInStoreActor], persistenceId), s"create-events-${storeCounter.incrementAndGet()}")
    val testProbe = TestProbe()
    testProbe watch storeEventsActor
    events foreach { event =>
      testProbe.send(storeEventsActor, Tagged(event, Set(tag)))
      testProbe.expectMsgType[Tagged]
    }
    storeEventsActor ! PoisonPill
    testProbe.expectTerminated(storeEventsActor).existenceConfirmed shouldBe true
    ()
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    storeEvents(initialEvents)
  }

  class TestReplayable(offsets: InMemoryBasedOffsetStore, withPartialReplay: Boolean = true)
      extends AbstractReplayableEventMaterializer(classicSystem, withPartialReplay) {
    override val logger: Logger         = Logger(LoggerFactory.getLogger(classOf[TestReplayable]))
    override val tagName: String        = tag
    override val matMappingName: String = viewId
    private val replayedEvents          = new ConcurrentLinkedQueue[Any]()
    private val liveEvents              = new ConcurrentLinkedQueue[Any]()

    def replayed: Seq[Any] = replayedEvents.asScala.toSeq
    def live: Seq[Any]     = liveEvents.asScala.toSeq

    override def handleReplayEvent(evt: Any): Future[Done] = {
      replayedEvents.add(evt)
      Future.successful(Done)
    }

    override def handleEvent(evt: Any): Future[Done] = {
      liveEvents.add(evt)
      Future.successful(Done)
    }

    override def saveOffset(viewIdentifier: String, offset: Offset): Future[Unit] =
      offsets.saveOffset(viewIdentifier, offset)

    override def getOffset(viewIdentifier: String): Future[Offset] = offsets.getOffset(viewIdentifier)
  }
}
