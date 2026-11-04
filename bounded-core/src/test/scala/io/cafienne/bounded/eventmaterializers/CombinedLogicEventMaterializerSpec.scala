/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.eventmaterializers

import io.cafienne.bounded.aggregate.AggregateType
import org.apache.pekko.Done
import org.apache.pekko.actor.{ActorSystem, ExtendedActorSystem, PoisonPill, Props}
import org.apache.pekko.actor.testkit.typed.scaladsl.{LogCapturing, ScalaTestWithActorTestKit}
import org.apache.pekko.persistence.journal.Tagged
import org.apache.pekko.persistence.query.{NoOffset, Offset}
import org.apache.pekko.testkit.TestProbe
import com.typesafe.scalalogging.Logger
import io.cafienne.bounded.aggregate.DomainEvent
import io.cafienne.bounded.eventmaterializers.offsetstores.InMemoryBasedOffsetStore
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.LoggerFactory

import java.util.concurrent.ConcurrentLinkedQueue
import scala.concurrent.Future
import scala.jdk.CollectionConverters._

case class CombinedLogicTestEvent(id: String, text: String) extends DomainEvent

/**
  * Characterizes the current behaviour of CombinedLogicEventMaterializer over two tags.
  */
class CombinedLogicEventMaterializerSpec
    extends ScalaTestWithActorTestKit(SpecConfig.testConfig)
    with AnyWordSpecLike
    with Matchers
    with Eventually
    with LogCapturing {

  implicit val classicSystem: ActorSystem = system.classicSystem

  val tagA = "combined-logic-tag-a"
  val tagB = "combined-logic-tag-b"

  val initialEvents = Seq(
    CombinedLogicTestEvent("combined-logic-ar-a", "a1") -> tagA,
    CombinedLogicTestEvent("combined-logic-ar-b", "b1") -> tagB,
    CombinedLogicTestEvent("combined-logic-ar-a", "a2") -> tagA,
    CombinedLogicTestEvent("combined-logic-ar-b", "b2") -> tagB
  )

  "The CombinedLogic Event Materializer" must {

    "handle the events of both tags, publish them and save an offset" in {
      val combinedLogic            = new TestCombinedLogic("combined-logic-both-tags")
      val (killSwitch, lastOffset) = combinedLogic.registerListenerWithKillSwitch(None)

      eventually {
        combinedLogic.handledEvents should contain theSameElementsAs initialEvents.map(_._1)
      }
      eventually {
        combinedLogic.offsets.getOffset(combinedLogic.viewIdentifier).futureValue should not be NoOffset
      }

      killSwitch.shutdown()
      lastOffset.futureValue should not be NoOffset
    }

    "publish an EventProcessed per handled event" in {
      val listener = TestProbe()
      classicSystem.eventStream.subscribe(listener.ref, classOf[EventProcessed])
      val combinedLogic   = new TestCombinedLogic("combined-logic-publish")
      val (killSwitch, _) = combinedLogic.registerListenerWithKillSwitch(None)

      val published = listener.receiveWhile(messages = initialEvents.size) {
        case e: EventProcessed if e.materializerId == combinedLogic.materializerId => e.evt
      }
      published should contain theSameElementsAs initialEvents.map(_._1)
      killSwitch.shutdown()
    }

    "start from the given offset instead of the stored one" in {
      val combinedLogic = new TestCombinedLogic("combined-logic-given-offset")
      combinedLogic.offsets.saveOffset(combinedLogic.viewIdentifier, Offset.sequence(Long.MaxValue)).futureValue
      val (killSwitch, _) = combinedLogic.registerListenerWithKillSwitch(Some(NoOffset))

      eventually {
        combinedLogic.handledEvents should contain allElementsOf initialEvents.map(_._1)
      }
      killSwitch.shutdown()
    }

    "handle events that are added while it is listening" in {
      val combinedLogic            = new TestCombinedLogic("combined-logic-live")
      val (killSwitch, lastOffset) = combinedLogic.registerListenerWithKillSwitch(None)
      eventually(combinedLogic.handledEvents should have size initialEvents.size.toLong)

      storeEvents("combined-logic-live-ar", Seq(CombinedLogicTestEvent("combined-logic-live-ar", "b3") -> tagB))

      eventually(combinedLogic.handledEvents should contain(CombinedLogicTestEvent("combined-logic-live-ar", "b3")))
      killSwitch.shutdown()
      lastOffset.futureValue should not be NoOffset
    }
  }

  private def storeEvents(persistenceId: String, events: Seq[(DomainEvent, String)]): Unit = {
    val storeEventsActor = classicSystem
      .asInstanceOf[ExtendedActorSystem]
      .systemActorOf(Props(classOf[CreateEventsInStoreActor], persistenceId), s"create-events-$persistenceId")
    val testProbe = TestProbe()
    testProbe watch storeEventsActor
    events foreach { case (event, tag) =>
      testProbe.send(storeEventsActor, Tagged(event, Set(tag)))
      testProbe.expectMsgType[Tagged]
    }
    storeEventsActor ! PoisonPill
    testProbe.expectTerminated(storeEventsActor).existenceConfirmed shouldBe true
    ()
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    initialEvents.groupBy(_._1.id).foreach { case (id, events) => storeEvents(id, events) }
  }

  class TestCombinedLogic(name: String) extends CombinedLogicEventMaterializer(classicSystem) {
    override val logger: Logger           = Logger(LoggerFactory.getLogger(classOf[TestCombinedLogic]))
    override val aggregateTypes           = List(AggregateType(tagA), AggregateType(tagB))
    override val matMappingName: String   = name
    val offsets: InMemoryBasedOffsetStore = new InMemoryBasedOffsetStore()
    private val handled                   = new ConcurrentLinkedQueue[Any]()

    def handledEvents: Seq[Any] = handled.asScala.toSeq

    override def handleEvent(evt: Any): Future[Done] = {
      handled.add(evt)
      Future.successful(Done)
    }

    override def saveOffset(viewIdentifier: String, offset: Offset): Future[Unit] =
      offsets.saveOffset(viewIdentifier, offset)

    override def getOffset(viewIdentifier: String): Future[Offset] = offsets.getOffset(viewIdentifier)
  }
}
