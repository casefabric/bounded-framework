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
import scala.jdk.CollectionConverters._

case class RestartTestEvent(id: String, text: String) extends DomainEvent

class EventMaterializerRestartSpec
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

  private val tagCounter = new AtomicInteger()

  "A listening event materializer" must {

    "restart after a failing handler and handle every event, only the failed one twice" in {
      val tag          = storeSixEvents()
      val materializer = new FlakyMaterializer(tag, failHandling = (text, attempt) => text == "e3" && attempt == 1)

      val (killSwitch, lastOffset) = materializer.registerListenerWithKillSwitch(Some(NoOffset))

      eventually(materializer.handled shouldBe texts(1 to 6))
      materializer.attempts("e1") shouldBe 1
      materializer.attempts("e2") shouldBe 1
      materializer.attempts("e3") shouldBe 2
      eventually(materializer.offsets.getOffset(materializer.viewIdentifier).futureValue shouldBe Sequence(6L))
      killSwitch.shutdown()
      lastOffset.futureValue shouldBe Sequence(6L)
    }

    "restart after a failing offset save and resume after the last handled event" in {
      val tag          = storeSixEvents()
      val materializer = new FlakyMaterializer(tag, failSave = (offset, attempt) => offset == Sequence(3L) && attempt == 1)

      val (killSwitch, _) = materializer.registerListenerWithKillSwitch(Some(NoOffset))

      eventually(materializer.offsets.getOffset(materializer.viewIdentifier).futureValue shouldBe Sequence(6L))
      // the stream resumes after the last handled event and a later save stores the position the failed one missed
      materializer.handled.distinct shouldBe texts(1 to 6)
      killSwitch.shutdown()
    }

    "resume after the last handled event when it does not keep offsets" in {
      val tag = storeSixEvents()
      val materializer =
        new FlakyMaterializer(tag, keepOffset = false, failHandling = (text, attempt) => text == "e5" && attempt == 1)

      val (killSwitch, _) = materializer.registerListenerWithKillSwitch(Some(NoOffset))

      eventually(materializer.handled shouldBe texts(1 to 6))
      (1 to 4).foreach(i => materializer.attempts(s"e$i") shouldBe 1)
      materializer.offsets.getOffset(materializer.viewIdentifier).futureValue shouldBe NoOffset
      killSwitch.shutdown()
    }

    "fail its future when the restarts are used up" in {
      val tag          = storeSixEvents()
      val materializer = new FlakyMaterializer(tag, failHandling = (text, _) => text == "e2")

      val failure = materializer.registerListener(Some(NoOffset)).failed.futureValue

      failure.getMessage shouldBe "handling e2 failed"
      materializer.attempts("e2") shouldBe 4 // the first attempt plus max-restarts = 3
    }

    "complete its future when it is stopped" in {
      val tag          = storeSixEvents()
      val materializer = new FlakyMaterializer(tag)

      val (killSwitch, lastOffset) = materializer.registerListenerWithKillSwitch(Some(NoOffset))
      eventually(materializer.handled should have size 6)
      killSwitch.shutdown()

      lastOffset.futureValue shouldBe Sequence(6L)
    }
  }

  "A replaying event materializer" must {

    "retry a failing replay from the last replayed event" in {
      val tag          = storeSixEvents()
      val materializer = new FlakyMaterializer(tag, failHandling = (text, attempt) => text == "e3" && attempt == 1)

      materializer.replayEvents().futureValue shouldBe Sequence(6L)

      materializer.handled shouldBe texts(1 to 6)
      materializer.attempts("e1") shouldBe 1
      materializer.attempts("e2") shouldBe 1
      materializer.attempts("e3") shouldBe 2
    }

    "fail when the retries are used up" in {
      val tag          = storeSixEvents()
      val materializer = new FlakyMaterializer(tag, failHandling = (text, _) => text == "e2")

      materializer.replayEvents().failed.futureValue.getMessage shouldBe "handling e2 failed"
      materializer.attempts("e2") shouldBe 4
    }
  }

  private def texts(range: Range): Seq[String] = range.map(i => s"e$i")

  /**
    * Stores e1..e6 under a tag of its own, so every test reads only its own events (and offsets 1..6).
    */
  private def storeSixEvents(): String = {
    val n             = tagCounter.incrementAndGet()
    val tag           = s"restart-tag-$n"
    val persistenceId = s"restart-ar-$n"
    val storeEventsActor = classicSystem
      .asInstanceOf[ExtendedActorSystem]
      .systemActorOf(Props(classOf[CreateEventsInStoreActor], persistenceId), s"create-events-$n")
    val testProbe = TestProbe()
    testProbe watch storeEventsActor
    texts(1 to 6).foreach { text =>
      testProbe.send(storeEventsActor, Tagged(RestartTestEvent(persistenceId, text), Set(tag)))
      testProbe.expectMsgType[Tagged]
    }
    storeEventsActor ! PoisonPill
    testProbe.expectTerminated(storeEventsActor)
    tag
  }

  /**
    * Materializer whose handler and offset store fail when told to. Attempts are counted per event, and the
    * offset store is its own, so the inmemory journal offsets 1..6 of the tag are what gets stored.
    */
  class FlakyMaterializer(
    tag: String,
    keepOffset: Boolean = true,
    failHandling: (String, Int) => Boolean = (_, _) => false,
    failSave: (Offset, Int) => Boolean = (_, _) => false
  ) extends AbstractReplayableEventMaterializer(classicSystem, keepOffset) {
    override val logger: Logger           = Logger(LoggerFactory.getLogger(classOf[FlakyMaterializer]))
    override val tagName: String          = tag
    override val matMappingName: String   = s"view-$tag"
    val offsets: InMemoryBasedOffsetStore = new InMemoryBasedOffsetStore()
    private val handledEvents             = new ConcurrentLinkedQueue[String]()
    private val handlingAttempts          = new ConcurrentHashMap[String, AtomicInteger]()
    private val saveAttempts              = new ConcurrentHashMap[Offset, AtomicInteger]()

    def handled: Seq[String]          = handledEvents.asScala.toSeq
    def attempts(text: String): Int = Option(handlingAttempts.get(text)).fold(0)(_.get)

    override def handleEvent(evt: Any): Future[Done] = {
      val text    = evt.asInstanceOf[RestartTestEvent].text
      val attempt = handlingAttempts.computeIfAbsent(text, _ => new AtomicInteger()).incrementAndGet()
      if (failHandling(text, attempt)) Future.failed(new RuntimeException(s"handling $text failed"))
      else {
        handledEvents.add(text)
        Future.successful(Done)
      }
    }

    override def handleReplayEvent(evt: Any): Future[Done] = handleEvent(evt)

    override def saveOffset(viewIdentifier: String, offset: Offset): Future[Unit] = {
      val attempt = saveAttempts.computeIfAbsent(offset, _ => new AtomicInteger()).incrementAndGet()
      if (failSave(offset, attempt)) Future.failed(new RuntimeException(s"saving $offset failed"))
      else offsets.saveOffset(viewIdentifier, offset)
    }

    override def getOffset(viewIdentifier: String): Future[Offset] = offsets.getOffset(viewIdentifier)
  }
}
