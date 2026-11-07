/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.readside

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import com.typesafe.config.ConfigFactory
import io.cafienne.bounded.aggregate.EventPosition
import org.scalatest.wordspec.AnyWordSpecLike

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration._

class ReadSideRegistrySpec
    extends ScalaTestWithActorTestKit(
      ConfigFactory
        .parseString("""
          bounded.eventmaterializers.publish = on
          bounded.read-side.timeout = 300ms
          bounded.read-side.idle-retention = 2s
        """)
        .withFallback(ConfigFactory.load())
    )
    with AnyWordSpecLike {

  private val registry = ReadSideRegistry(system.classicSystem)
  private val counter  = new AtomicInteger()

  /** A read side and persistence id of their own per test. */
  private def fresh(): (String, String) = {
    val n  = counter.incrementAndGet()
    val id = s"view-$n"
    registry.register(id)
    (id, s"notes|aggregate-$n")
  }

  private def publish(signal: ReadSideSignal): Unit = system.classicSystem.eventStream.publish(signal)

  private val boom = new RuntimeException("boom")

  "The read-side registry" should {
    "complete a waiter once the read side has processed its position" in {
      val (id, pid) = fresh()
      val waiting   = registry.awaitProcessed(id, EventPosition(pid, 2), 5.seconds)
      publish(ProjectionProgress(id, pid, 1))
      publish(ProjectionProgress(id, pid, 2))
      waiting.futureValue shouldBe Processed
    }

    "complete at once for a position the read side already processed" in {
      val (id, pid) = fresh()
      publish(ProjectionProgress(id, pid, 3))
      eventually(
        registry.awaitProcessed(id, EventPosition(pid, 2), 5.seconds).value.flatMap(_.toOption) shouldBe Some(Processed)
      )
    }

    "not complete for another persistence id or another read side" in {
      val (id, pid)  = fresh()
      val (other, _) = fresh()
      val waiting    = registry.awaitProcessed(id, EventPosition(pid, 1))
      publish(ProjectionProgress(id, "notes|someone-else", 1))
      publish(ProjectionProgress(other, pid, 1))
      waiting.futureValue shouldBe Pending(Pending.TimedOut)
    }

    "answer Pending(TimedOut) after the timeout" in {
      val (id, pid) = fresh()
      registry.awaitProcessed(id, EventPosition(pid, 1), 100.millis).futureValue shouldBe Pending(Pending.TimedOut)
    }

    "answer Failed when the read side gives up on the event or one before it, and not for a failure it retries" in {
      val (id, pid) = fresh()
      val before    = registry.awaitProcessed(id, EventPosition(pid, 1), 5.seconds)
      val atFailed  = registry.awaitProcessed(id, EventPosition(pid, 2), 5.seconds)
      val after     = registry.awaitProcessed(id, EventPosition(pid, 3), 5.seconds)
      publish(ProjectionProgress(id, pid, 1))
      publish(ProjectionFailure(id, pid, 2, boom, isFinal = false))
      before.futureValue shouldBe Processed
      Thread.sleep(100)
      atFailed.isCompleted shouldBe false
      publish(ProjectionFailure(id, pid, 2, boom, isFinal = true))
      atFailed.futureValue shouldBe Failed(boom)
      after.futureValue shouldBe Failed(boom)
    }

    "answer Failed at once to a waiter that comes after the read side gave up, until it processes the event after all" in {
      val (id, pid) = fresh()
      publish(ProjectionFailure(id, pid, 2, boom, isFinal = true))
      eventually(
        registry.awaitProcessed(id, EventPosition(pid, 2)).value.flatMap(_.toOption) shouldBe Some(Failed(boom))
      )
      publish(ProjectionProgress(id, pid, 2))
      eventually(registry.awaitProcessed(id, EventPosition(pid, 2)).value.flatMap(_.toOption) shouldBe Some(Processed))
    }

    "answer Pending(Down) to every waiter of a read side that stops" in {
      val (id, pid)  = fresh()
      val (other, _) = fresh()
      val first      = registry.awaitProcessed(id, EventPosition(pid, 1), 5.seconds)
      val second     = registry.awaitProcessed(id, EventPosition("notes|another", 4), 5.seconds)
      val unaffected = registry.awaitProcessed(other, EventPosition(pid, 1), 5.seconds)
      publish(ProjectionDown(id, boom))
      first.futureValue shouldBe Pending(Pending.Down(boom))
      second.futureValue shouldBe Pending(Pending.Down(boom))
      publish(ProjectionProgress(other, pid, 1))
      unaffected.futureValue shouldBe Processed
    }

    "drop what it knows of a persistence id after the idle retention" in {
      val (id, pid) = fresh()
      publish(ProjectionProgress(id, pid, 1))
      eventually(registry.awaitProcessed(id, EventPosition(pid, 1)).value.flatMap(_.toOption) shouldBe Some(Processed))
      Thread.sleep(4000)
      registry.awaitProcessed(id, EventPosition(pid, 1), 100.millis).futureValue shouldBe Pending(Pending.TimedOut)
    }

    "fail the future for a read side that is not registered" in {
      registry
        .awaitProcessed("no-such-view", EventPosition("notes|1", 1))
        .failed
        .futureValue shouldBe an[IllegalArgumentException]
    }

    "fail the future when publishing is off" in {
      val quiet = ActorSystem("quiet", ConfigFactory.parseString("bounded.eventmaterializers.publish = off"))
      try {
        val quietRegistry = ReadSideRegistry(quiet)
        quietRegistry.register("view")
        quietRegistry
          .awaitProcessed("view", EventPosition("notes|1", 1))
          .failed
          .futureValue shouldBe an[IllegalStateException]
      } finally quiet.terminate(): Unit
    }

    "fail its waiters and later waits when the actor system terminates" in {
      val other = ActorSystem("terminating", ConfigFactory.parseString("bounded.eventmaterializers.publish = on"))
      val otherRegistry = ReadSideRegistry(other)
      otherRegistry.register("view")
      val waiting = otherRegistry.awaitProcessed("view", EventPosition("notes|1", 1), 1.minute)
      other.terminate().futureValue: Unit
      waiting.failed.futureValue shouldBe an[IllegalStateException]
      otherRegistry
        .awaitProcessed("view", EventPosition("notes|1", 1))
        .failed
        .futureValue shouldBe an[IllegalStateException]
    }
  }
}
