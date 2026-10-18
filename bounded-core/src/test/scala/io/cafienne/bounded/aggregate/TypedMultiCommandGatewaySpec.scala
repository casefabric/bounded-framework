/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.aggregate

import java.time.OffsetDateTime
import org.apache.pekko.actor.testkit.typed.scaladsl.{LogCapturing, ScalaTestWithActorTestKit}
import org.apache.pekko.actor.typed.{ActorSystem, Scheduler}
import org.apache.pekko.util.Timeout
import io.cafienne.bounded.aggregate.typed.DefaultTypedCommandGateway
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AsyncFlatSpecLike
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor, Future}

/**
  * Two typed command gateways, each for its own type of aggregate root (with its own protocol),
  * running next to each other in the same actor system.
  */
class TypedMultiCommandGatewaySpec extends ScalaTestWithActorTestKit(ConfigFactory.parseString(s"""
    pekko.persistence.publish-plugin-commands = on
    pekko.persistence.journal.plugin = "pekko.persistence.journal.inmem"
    pekko.persistence.journal.inmem.test-serialization = on
    pekko.actor.warn-about-java-serializer-usage = off
    # snapshot store plugin is NOT defined, things should still work
    pekko.persistence.snapshot-store.local.dir = "target/snapshots-${classOf[TypedMultiCommandGatewaySpec].getName}/"
    #PLEASE NOTE THAT CoordinatedShutdown needs to be disabled as below in order to run the test properly
    #SEE https://pekko.apache.org/docs/pekko/current/coordinated-shutdown.html at the end of the page
    pekko.coordinated-shutdown.terminate-actor-system = off
    pekko.coordinated-shutdown.run-by-actor-system-terminate = off
    pekko.coordinated-shutdown.run-by-jvm-shutdown-hook = off
    pekko.cluster.run-coordinated-shutdown-when-down = off
    """)) with LogCapturing with AsyncFlatSpecLike with ScalaFutures with BeforeAndAfterAll {

  import TypedSimpleAggregate._
  import TypedAnotherAggregate._

  implicit val simpleCommandValidator: ValidateableCommand[SimpleAggregateCommand] =
    new ValidateableCommand[SimpleAggregateCommand] {
      override def validate(cmd: SimpleAggregateCommand): Future[SimpleAggregateCommand] =
        Future.successful(cmd)
    }

  implicit val anotherCommandValidator: ValidateableCommand[AnotherAggregateCommand] =
    new ValidateableCommand[AnotherAggregateCommand] {
      override def validate(cmd: AnotherAggregateCommand): Future[AnotherAggregateCommand] =
        Future.successful(cmd)
    }

  implicit val actorSystem: ActorSystem[_]  = system
  implicit val ec: ExecutionContextExecutor = system.executionContext
  implicit val scheduler: Scheduler         = system.scheduler

  behavior of "Multiple Typed Command Gateways"

  implicit val gatewayTimeout: Timeout = Timeout(10.seconds)

  val commandMetaData = AggregateCommandMetaData(OffsetDateTime.now(), None)

  val simpleGateway =
    new DefaultTypedCommandGateway[SimpleAggregateCommand](system, new SimpleAggregateManager(), 6.seconds)
  val anotherGateway =
    new DefaultTypedCommandGateway[AnotherAggregateCommand](system, new AnotherAggregateManager(), 6.seconds)

  "Command Gateway" should "be able to be created multiple times to route to other aggregates" in {
    assert(simpleGateway != anotherGateway)
  }

  "Command Gateway" should "be able to send commands to multiple aggregates" in {
    val probe = testKit.createTestProbe[Response]()

    whenReady(anotherGateway.tell(AddAnother("other1", commandMetaData, "other item 1", probe.ref))) { answer =>
      answer shouldEqual (())
      probe.expectMessage(OK) shouldEqual OK
    }

    whenReady(simpleGateway.tell(AddItem("ar1", commandMetaData, "item 1", probe.ref))) { answer =>
      answer shouldEqual (())
      probe.expectMessage(OK) shouldEqual OK
    }
  }

  "Command Gateway" should "keep the state of each aggregate type apart" in {
    for {
      anotherItems <- anotherGateway.ask("other1", ref => GetAnotherItems("other1", commandMetaData, ref))
      simpleItems  <- simpleGateway.ask("ar1", ref => GetItems("ar1", commandMetaData, ref))
    } yield {
      anotherItems shouldEqual Items(List("other item 1"))
      simpleItems shouldEqual Items(List("item 1"))
    }
  }

  protected override def afterAll(): Unit = {
    Await.ready(simpleGateway.shutdown(), 5.seconds)
    Await.ready(anotherGateway.shutdown(), 5.seconds)
    super.afterAll()
  }

}
