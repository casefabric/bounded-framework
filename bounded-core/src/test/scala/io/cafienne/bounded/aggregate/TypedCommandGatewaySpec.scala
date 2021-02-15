/*
 * Copyright (C) 2016-2021 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.aggregate

import java.time.{OffsetDateTime, ZonedDateTime}

import akka.actor.testkit.typed.javadsl.LoggingTestKit
import akka.actor.testkit.typed.scaladsl.{LogCapturing, ManualTime, ScalaTestWithActorTestKit}
import akka.actor.typed.ActorSystem
import akka.util.Timeout
import io.cafienne.bounded.{BuildInfo, RuntimeInfo}
import io.cafienne.bounded.aggregate.typed.DefaultTypedCommandGateway
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AsyncFlatSpecLike

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class TypedCommandGatewaySpec extends ScalaTestWithActorTestKit(s"""
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
    """) with AsyncFlatSpecLike with ScalaFutures with BeforeAndAfterAll { // with LogCapturing {

  import TypedSimpleAggregate._

  implicit val commandValidator = new ValidateableCommand[SimpleAggregateCommand] {
    override def validate(cmd: SimpleAggregateCommand): Future[SimpleAggregateCommand] =
      Future.successful(cmd)
  }

  implicit val actorSystem: ActorSystem[_] = system
  implicit val ec                          = system.executionContext
  implicit val scheduler                   = system.scheduler

  behavior of "Typed Command Gateway"

  implicit val gatewayTimeout = Timeout(10.seconds)
  implicit val buildInfo      = BuildInfo("spec", "1.0")
  implicit val runtimeInfo    = RuntimeInfo("current")

  val commandMetaData     = AggregateCommandMetaData(OffsetDateTime.now(), None)
  val creator             = new SimpleAggregateManager()
  val typedCommandGateway = new DefaultTypedCommandGateway[SimpleAggregateCommand](system, creator)

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
    whenReady(typedCommandGateway.tell(Stop(aggregateId, commandMetaData, probe.ref))) { answer =>
      answer shouldEqual (())
      probe.expectMessage(OK)
    }
    Thread.sleep(3000)
    whenReady(typedCommandGateway.tell(AddItem(aggregateId, commandMetaData, "new item 2", probe.ref))) { answer =>
      answer shouldEqual (())
      val probeAnswer = probe.expectMessage(OK)
      assert(probeAnswer.equals(OK))
    }
  }

  "Command Gateway" should "recover after the aggregate stopped itself" in {
    val commandMetaData = AggregateCommandMetaData(OffsetDateTime.now(), None)
    val aggregateId     = "test0"
    val probe           = testKit.createTestProbe[Response]()
    whenReady(typedCommandGateway.tell(StopAfter(aggregateId, commandMetaData, 3, probe.ref))) { answer =>
      answer shouldEqual (())
      val probeAnswer = probe.expectMessage(OK)
      assert(probeAnswer.equals(OK))
    }
    whenReady(typedCommandGateway.tell(AddItem(aggregateId, commandMetaData, "new item 3", probe.ref))) { answer =>
      answer shouldEqual (())
      val probeAnswer2 = probe.expectMessage(OK)
      assert(probeAnswer2.equals(OK))
    }
  }

  protected override def afterAll(): Unit = {
    Await.ready(typedCommandGateway.shutdown(), 5.seconds)
  }

}
