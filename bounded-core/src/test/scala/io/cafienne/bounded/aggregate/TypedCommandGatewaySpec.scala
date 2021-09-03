/*
 * Copyright (C) 2016-2021 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.aggregate

import java.time.OffsetDateTime
import akka.actor.testkit.typed.scaladsl.{
  LogCapturing,
  LoggingTestKit,
  ManualTime,
  ScalaTestWithActorTestKit,
  TestProbe
}
import akka.actor.typed.ActorSystem
import akka.util.Timeout
import io.cafienne.bounded.aggregate.typed.DefaultTypedCommandGateway
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AsyncFlatSpecLike
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class TypedCommandGatewaySpec
    extends ScalaTestWithActorTestKit(ManualTime.config.withFallback(ConfigFactory.parseString(s"""
    akka.persistence.publish-plugin-commands = on
    akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
    akka.persistence.journal.inmem.test-serialization = on
    akka.actor.warn-about-java-serializer-usage = off
    # snapshot store plugin is NOT defined, things should still work
    akka.persistence.snapshot-store.local.dir = "target/snapshots-${classOf[TypedCommandGatewaySpec].getName}/"
    #PLEASE NOTE THAT CoordinatedShutdown needs to be disabled as below in order to run the test properly
    #SEE https://doc.akka.io/docs/akka/current/coordinated-shutdown.html at the end of the page
    akka.coordinated-shutdown.terminate-actor-system = off
    akka.coordinated-shutdown.run-by-actor-system-terminate = off
    akka.coordinated-shutdown.run-by-jvm-shutdown-hook = off
    akka.cluster.run-coordinated-shutdown-when-down = off
    """)))
    with LogCapturing
    with AsyncFlatSpecLike
    with ScalaFutures
    with BeforeAndAfterAll {

  import TypedSimpleAggregate._

  implicit val commandValidator = new ValidateableCommand[SimpleAggregateCommand] {
    override def validate(cmd: SimpleAggregateCommand): Future[SimpleAggregateCommand] =
      Future.successful(cmd)
  }

  implicit val actorSystem: ActorSystem[_] = system
  implicit val ec                          = system.executionContext
  implicit val scheduler                   = system.scheduler

  val manualTime: ManualTime = ManualTime()

  behavior of "Typed Command Gateway"

  implicit val gatewayTimeout = Timeout(10.seconds)

  val commandMetaData     = AggregateCommandMetaData(OffsetDateTime.now(), None)
  val creator             = new SimpleAggregateManager()
  val typedCommandGateway = new DefaultTypedCommandGateway[SimpleAggregateCommand](system, creator, 6.seconds)

  "Command Gateway" should "send Create command" in {
    val commandMetaData = AggregateCommandMetaData(OffsetDateTime.now(), None)
    val aggregateId     = "test0"
    val probe           = testKit.createTestProbe[Response]()
    whenReady(typedCommandGateway.tell(Create(aggregateId, commandMetaData, probe.ref))) { answer =>
      answer shouldEqual (())
      val probeAnswer = probe.expectMessage(OK)
      assert(probeAnswer.equals(OK))
    }

  }

  "Command Gateway" should "work with send and ask" in {
    val commandMetaData = AggregateCommandMetaData(OffsetDateTime.now(), None)
    val aggregateId     = "testask"
    whenReady(typedCommandGateway.ask(aggregateId, ref => Create(aggregateId, commandMetaData, ref))) {
      answer: Response => answer shouldEqual (OK)
    }
  }

  "Command Gateway" should "handle second message via actor in the Map" in {
    val commandMetaData = AggregateCommandMetaData(OffsetDateTime.now(), None)
    val aggregateId     = "test0"
    val probe           = testKit.createTestProbe[Response]()
    whenReady(typedCommandGateway.tell(AddItem(aggregateId, commandMetaData, "new item 1", probe.ref))) { answer =>
      answer shouldEqual (())
      val probeAnswer = probe.expectMessage(OK)
      assert(probeAnswer.equals(OK))
    }
  }

  "Command Gateway" should "recover after stop message and add to the map" in {
    val commandMetaData = AggregateCommandMetaData(OffsetDateTime.now(), None)
    val aggregateId     = "test0"
    val probe           = testKit.createTestProbe[Response]()
    LoggingTestKit.debug(s"Aggregate $aggregateId terminated").expect {
      whenReady(typedCommandGateway.tell(Stop(aggregateId, commandMetaData, probe.ref))) { answer =>
        answer shouldEqual (())
        probe.expectMessage(OK)
      }
      manualTime.timePasses(1.seconds)
    }
    whenReady(typedCommandGateway.tell(AddItem(aggregateId, commandMetaData, "new item 2", probe.ref))) { answer =>
      answer shouldEqual (())
      val probeAnswer = probe.expectMessage(OK)
      assert(probeAnswer.equals(OK))
    }
  }

  "Command Gateway" should "stop an actor after the passivation timeout has been met" in {
    val commandMetaData = AggregateCommandMetaData(OffsetDateTime.now(), None)
    val aggregateId     = "testPassivate"
    val probe           = testKit.createTestProbe[Response]()
    //start the actor

    LoggingTestKit.debug(s"Aggregate $aggregateId terminated").expect {
      whenReady(typedCommandGateway.tell(Stop(aggregateId, commandMetaData, probe.ref))) { answer =>
        answer shouldEqual (())
        probe.expectMessage(OK)
      }
      manualTime.expectNoMessageFor(6.seconds)
      manualTime.timePasses(2.millis)
      //Assertion required although the terminated message is what is been asserted.
      assert(true)
    }
  }

  "Command Gateway" should "recover after the aggregate stopped itself" in {
    val commandMetaData = AggregateCommandMetaData(OffsetDateTime.now(), None)
    val aggregateId     = "test0"
    val probe           = testKit.createTestProbe[Response]()
    LoggingTestKit.debug(s"Aggregate $aggregateId terminated").expect {
      whenReady(typedCommandGateway.tell(StopAfter(aggregateId, commandMetaData, 3, probe.ref))) { answer =>
        answer shouldEqual (())
        val probeAnswer = probe.expectMessage(OK)
        assert(probeAnswer.equals(OK))
      }
      manualTime.timePasses(4.seconds)
    }

    whenReady(typedCommandGateway.tell(AddItem(aggregateId, commandMetaData, "new item 3", probe.ref))) { answer =>
      answer shouldEqual (())
      val probeAnswer2 = probe.expectMessage(OK)
      assert(probeAnswer2.equals(OK))
    }
  }

  "Command Gateway" should "be able to be created multiple times to route to other aggregates" in {
    val creator                    = new AnotherAggregateManager()
    val anotherTypedCommandGateway = new DefaultTypedCommandGateway[SimpleAggregateCommand](system, creator, 6.seconds)

    assert(anotherTypedCommandGateway != null)
  }

  protected override def afterAll(): Unit = {
    Await.ready(typedCommandGateway.shutdown(), 5.seconds).map(_ => (): Unit)
  }

}
