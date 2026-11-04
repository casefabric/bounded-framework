/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.eventmaterializers

import io.cafienne.bounded.aggregate.AggregateType
import org.apache.pekko.Done
import org.apache.pekko.actor.{ActorSystem, ExtendedActorSystem, PoisonPill, Props}
import org.apache.pekko.actor.testkit.typed.scaladsl.{LogCapturing, ScalaTestWithActorTestKit}
import org.apache.pekko.persistence.journal.Tagged
import org.apache.pekko.persistence.query.{Offset, Sequence}
import org.apache.pekko.testkit.TestProbe
import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.Logger
import io.cafienne.bounded.aggregate.DomainEvent
import io.cafienne.bounded.eventmaterializers.offsetstores.InMemoryBasedOffsetStore
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.LoggerFactory

import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue}
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

case class CombinedLogicTagEvent(id: String, text: String) extends DomainEvent

/**
  * A combined logic keeps its position per tag (TagOffsets). The inmemory journal numbers the offsets of every tag
  * separately, like LevelDB, so one shared offset would skip events.
  */
class CombinedLogicTagOffsetsSpec
    extends ScalaTestWithActorTestKit(
      ConfigFactory
        .parseString("""
          bounded.eventmaterializers.restart {
            min-backoff = 10ms
            max-backoff = 50ms
            random-factor = 0
            max-restarts = 3
            max-restarts-within = 1m
          }
        """)
        .withFallback(SpecConfig.testConfig)
    )
    with AnyWordSpecLike
    with Matchers
    with LogCapturing {

  implicit val classicSystem: ActorSystem = system.classicSystem

  private val counter = new AtomicInteger()

  "A combined logic" must {

    "resume every tag from its own offset" in {
      val (tagA, tagB, tagC) = (newTag(), newTag(), newTag())
      store(tagA, "a1", "a2", "a3", "a4", "a5")
      store(tagB, "b1")
      store(tagC, "c1")
      val offsets = new InMemoryBasedOffsetStore()

      val first           = new TestCombinedLogic(List(tagA, tagB, tagC), offsets)
      val (killSwitch, _) = first.registerListenerWithKillSwitch(None)
      eventually(first.handled should contain theSameElementsAs Seq("a1", "a2", "a3", "a4", "a5", "b1", "c1"))
      eventually {
        offsets.getOffset(first.viewIdentifier).futureValue shouldBe
          TagOffsets(Map(tagA -> Sequence(5L), tagB -> Sequence(1L), tagC -> Sequence(1L)))
      }
      killSwitch.shutdown()

      store(tagB, "b2")
      store(tagC, "c2")
      val second           = new TestCombinedLogic(List(tagA, tagB, tagC), offsets)
      val (killSwitch2, _) = second.registerListenerWithKillSwitch(None)

      eventually(second.handled should contain theSameElementsAs Seq("b2", "c2"))
      Thread.sleep(300)
      second.handled should have size 2
      killSwitch2.shutdown()
    }

    "listen to a single tag" in {
      val tag = newTag()
      store(tag, "s1", "s2")
      val offsets       = new InMemoryBasedOffsetStore()
      val combinedLogic = new TestCombinedLogic(List(tag), offsets)

      val (killSwitch, lastOffset) = combinedLogic.registerListenerWithKillSwitch(None)

      eventually(combinedLogic.handled shouldBe Seq("s1", "s2"))
      // the event is handled before its offset passes; stop only once it has
      eventually(
        offsets.getOffset(combinedLogic.viewIdentifier).futureValue shouldBe TagOffsets(Map(tag -> Sequence(2L)))
      )
      killSwitch.shutdown()
      lastOffset.futureValue shouldBe TagOffsets(Map(tag -> Sequence(2L)))
    }

    "start every tag from a plain offset stored by an older version, and store TagOffsets from then on" in {
      val (tagA, tagB) = (newTag(), newTag())
      store(tagA, "a1", "a2")
      store(tagB, "b1", "b2")
      val offsets       = new InMemoryBasedOffsetStore()
      val combinedLogic = new TestCombinedLogic(List(tagA, tagB), offsets)
      offsets.saveOffset(combinedLogic.viewIdentifier, Sequence(1L)).futureValue

      val (killSwitch, _) = combinedLogic.registerListenerWithKillSwitch(None)

      eventually(combinedLogic.handled should contain theSameElementsAs Seq("a2", "b2"))
      eventually {
        offsets.getOffset(combinedLogic.viewIdentifier).futureValue shouldBe
          TagOffsets(Map(tagA -> Sequence(2L), tagB -> Sequence(2L)))
      }
      killSwitch.shutdown()
    }

    "restart after a failing handler without skipping or repeating events of the other tags" in {
      val (tagA, tagB) = (newTag(), newTag())
      store(tagA, "a1", "a2", "a3")
      store(tagB, "b1", "b2", "b3")
      val combinedLogic = new TestCombinedLogic(List(tagA, tagB), new InMemoryBasedOffsetStore(), failOnce = Set("b2"))

      val (killSwitch, _) = combinedLogic.registerListenerWithKillSwitch(None)

      eventually(combinedLogic.handled should contain theSameElementsAs Seq("a1", "a2", "a3", "b1", "b2", "b3"))
      combinedLogic.attempts("b2") shouldBe 2
      (combinedLogic.handled.toSet - "b2").foreach(text => combinedLogic.attempts(text) shouldBe 1)
      killSwitch.shutdown()
    }

    "refuse to start without tags" in {
      val combinedLogic = new TestCombinedLogic(List.empty, new InMemoryBasedOffsetStore())

      an[IllegalArgumentException] should be thrownBy combinedLogic.registerListenerWithKillSwitch(None)
    }
  }

  private def newTag(): String = s"combined-logic-offsets-${counter.incrementAndGet()}"

  /** Stores the events under the tag, as one persistence id per tag. */
  private def store(tag: String, texts: String*): Unit = {
    val storeEventsActor = classicSystem
      .asInstanceOf[ExtendedActorSystem]
      .systemActorOf(Props(classOf[CreateEventsInStoreActor], s"ar-$tag"), s"create-$tag-${counter.incrementAndGet()}")
    val testProbe = TestProbe()
    testProbe watch storeEventsActor
    texts.foreach { text =>
      testProbe.send(storeEventsActor, Tagged(CombinedLogicTagEvent(s"ar-$tag", text), Set(tag)))
      testProbe.expectMsgType[Tagged]
    }
    storeEventsActor ! PoisonPill
    testProbe.expectTerminated(storeEventsActor, 3.seconds).existenceConfirmed shouldBe true
    ()
  }

  class TestCombinedLogic(tags: List[String], offsets: InMemoryBasedOffsetStore, failOnce: Set[String] = Set.empty)
      extends CombinedLogicEventMaterializer(classicSystem) {
    override val logger: Logger         = Logger(LoggerFactory.getLogger(classOf[TestCombinedLogic]))
    override val aggregateTypes         = tags.map(AggregateType(_))
    override val matMappingName: String = s"combined-logic-view-${tags.mkString("-")}"
    private val handledEvents           = new ConcurrentLinkedQueue[String]()
    private val handlingAttempts        = new ConcurrentHashMap[String, AtomicInteger]()

    def handled: Seq[String]        = handledEvents.asScala.toSeq
    def attempts(text: String): Int = Option(handlingAttempts.get(text)).fold(0)(_.get)

    override def handleEvent(evt: Any): Future[Done] = {
      val text    = evt.asInstanceOf[CombinedLogicTagEvent].text
      val attempt = handlingAttempts.computeIfAbsent(text, _ => new AtomicInteger()).incrementAndGet()
      if (failOnce.contains(text) && attempt == 1) Future.failed(new RuntimeException(s"handling $text failed"))
      else {
        handledEvents.add(text)
        Future.successful(Done)
      }
    }

    override def saveOffset(viewIdentifier: String, offset: Offset): Future[Unit] =
      offsets.saveOffset(viewIdentifier, offset)

    override def getOffset(viewIdentifier: String): Future[Offset] = offsets.getOffset(viewIdentifier)
  }
}
