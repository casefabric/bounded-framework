/*
 * Copyright (C) 2016-2022 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.aggregate

import akka.actor.ActorSystem
import akka.testkit.{EventFilter, TestKit}
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AsyncFlatSpecLike
import org.scalatest.matchers.should.Matchers

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import org.scalatest.time.{Millis, Seconds, Span}

class ClassicCommandGatewaySpec
    extends TestKit(
      ActorSystem(
        "ClassicCommandGatewaySpec",
        ConfigFactory.parseString(s"""
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
    akka.loggers = ["akka.testkit.TestEventListener"]
    """)
      )
    )
    with AsyncFlatSpecLike
    with ScalaFutures
    with Matchers
    with BeforeAndAfterAll {

  //Setup required supporting classes
  implicit val timeout         = Timeout(10.seconds)
  implicit val defaultPatience = PatienceConfig(timeout = Span(10, Seconds), interval = Span(100, Millis))
  implicit val ec              = ExecutionContext.global

  val classicSimpleCreator = new ClassicSimpleAggregateCreator(system)
  val commandGateway       = new RouterCommandGateway(system, 2.seconds, classicSimpleCreator)

  import akka.pattern.ask
  import ClassicSimpleAggregate._

  //All commands are valid in this test
  implicit val commandValidator = new ValidateableCommand[Create] {
    override def validate(cmd: Create): Future[Create] =
      Future.successful(cmd)
  }

  implicit val commandValidator2 = new ValidateableCommand[AddItem] {
    override def validate(cmd: AddItem): Future[AddItem] =
      Future.successful(cmd)
  }

  implicit val commandValidator3 = new ValidateableCommand[GetItems] {
    override def validate(cmd: GetItems): Future[GetItems] =
      Future.successful(cmd)
  }

  behavior of "Classic Command Gateway"

  "Classic Simple Aggregate" should "work as expected" in {
    val aggregateId  = "testAr0"
    val aggregateRef = system.actorOf(classicSimpleCreator.props(aggregateId))
    whenReady(aggregateRef.ask(Create(aggregateId))) { answer => assert(answer == Ok(List(Created("testAr0")))) }
    assert(true)
  }

  "Command Gateway" should "hanlde multiple commands to a single aggregate" in {
    val testAggregateRootId1 = "testAr1"

    whenReady(commandGateway.send(Create(testAggregateRootId1))) { answer => assert(answer == ()) }

    whenReady(commandGateway.sendAndAsk(AddItem(testAggregateRootId1, "item1"))) { answer =>
      assert(answer == Ok(List(ItemAdded("testAr1", "item1"))))
    }

    assert(true)
  }

  it should "stop and restart an aggregate root when idle time has passed" in {

    val testAggregateRootId1 = "testAr1"

    EventFilter.debug(start = "Received Terminated for Actor", occurrences = 1).intercept {
      commandGateway.send(AddItem(testAggregateRootId1, "item2"))
    }

    whenReady(commandGateway.sendAndAsk(GetItems(testAggregateRootId1))) { answer =>
      assert(answer == Ko(Items(Seq("item1", "item2"))))
    }

    assert(true)
  }

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system, 30.seconds, verifySystemShutdown = true)
  }

}
