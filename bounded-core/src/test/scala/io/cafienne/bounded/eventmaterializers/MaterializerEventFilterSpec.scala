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
import scala.concurrent.Future
import scala.jdk.CollectionConverters._

case class FilterTestEvent(id: String, text: String, keep: Boolean) extends DomainEvent

/** An event under the tag that is not a DomainEvent. */
case class NonDomainEvent(text: String)

object KeepFilter extends MaterializerEventFilter {
  override def filter(evt: DomainEvent): Boolean = evt match {
    case e: FilterTestEvent => e.keep
    case _                  => false
  }
}

class MaterializerEventFilterSpec
    extends ScalaTestWithActorTestKit(SpecConfig.testConfig)
    with AnyWordSpecLike
    with Matchers
    with LogCapturing {

  implicit val classicSystem: ActorSystem = system.classicSystem

  val tag           = "filter-tag"
  val persistenceId = "filter-ar"

  // offsets 1..6; only the non-domain event (2) and d3 (4) pass the filter
  val events: Seq[Any] = Seq(
    FilterTestEvent(persistenceId, "d1", keep = false),
    NonDomainEvent("plain"),
    FilterTestEvent(persistenceId, "d2", keep = false),
    FilterTestEvent(persistenceId, "d3", keep = true),
    FilterTestEvent(persistenceId, "d4", keep = false),
    FilterTestEvent(persistenceId, "d5", keep = false)
  )
  val expectedHandled: Seq[Any] = Seq(NonDomainEvent("plain"), FilterTestEvent(persistenceId, "d3", keep = true))

  "An event materializer with a filter" must {

    "replay only accepted and non-domain events, and save the offset of filtered ones too" in {
      val materializer = new FilteringMaterializer("filter-replay-view")

      materializer.replayEvents().futureValue shouldBe Sequence(6L)

      materializer.handled shouldBe expectedHandled
      materializer.offsets.getOffset(materializer.viewIdentifier).futureValue shouldBe Sequence(6L)
    }

    "listen to only accepted and non-domain events, and save the offset of filtered ones too" in {
      val materializer = new FilteringMaterializer("filter-live-view")
      val listener     = TestProbe()
      classicSystem.eventStream.subscribe(listener.ref, classOf[EventProcessed])

      val (killSwitch, lastOffset) = materializer.registerListenerWithKillSwitch(Some(NoOffset))

      eventually(materializer.offsets.getOffset(materializer.viewIdentifier).futureValue shouldBe Sequence(6L))
      materializer.handled shouldBe expectedHandled
      val published = listener.receiveWhile(messages = 2) {
        case e: EventProcessed if e.materializerId == materializer.materializerId => e.evt
      }
      published shouldBe expectedHandled
      listener.expectNoMessage()
      killSwitch.shutdown()
      lastOffset.futureValue shouldBe Sequence(6L)
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    val storeEventsActor = classicSystem
      .asInstanceOf[ExtendedActorSystem]
      .systemActorOf(Props(classOf[CreateEventsInStoreActor], persistenceId), "create-filter-events")
    val testProbe = TestProbe()
    testProbe watch storeEventsActor
    events.foreach { event =>
      testProbe.send(storeEventsActor, Tagged(event, Set(tag)))
      testProbe.expectMsgType[Tagged]
    }
    storeEventsActor ! PoisonPill
    testProbe.expectTerminated(storeEventsActor).existenceConfirmed shouldBe true
    ()
  }

  class FilteringMaterializer(view: String) extends AbstractReplayableEventMaterializer(classicSystem, true, KeepFilter) {
    override val logger: Logger           = Logger(LoggerFactory.getLogger(classOf[FilteringMaterializer]))
    override val tagName: String          = tag
    override val matMappingName: String   = view
    val offsets: InMemoryBasedOffsetStore = new InMemoryBasedOffsetStore()
    private val handledEvents             = new ConcurrentLinkedQueue[Any]()

    def handled: Seq[Any] = handledEvents.asScala.toSeq

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
