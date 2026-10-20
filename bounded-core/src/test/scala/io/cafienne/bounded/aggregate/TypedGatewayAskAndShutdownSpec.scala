/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.aggregate

import java.time.OffsetDateTime
import java.util.concurrent.atomic.AtomicBoolean
import org.apache.pekko.actor.testkit.typed.scaladsl.{LogCapturing, ScalaTestWithActorTestKit}
import org.apache.pekko.actor.typed.{ActorRef, Behavior, PostStop, Scheduler}
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.cluster.sharding.typed.scaladsl.EntityTypeKey
import org.apache.pekko.util.Timeout
import io.cafienne.bounded.aggregate.typed.{DefaultTypedCommandGateway, TypedAggregateRootManager}
import com.typesafe.config.ConfigFactory
import org.scalatest.flatspec.AnyFlatSpecLike

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

object TypedGatewayAskAndShutdownSpec {
  final case class Ping(aggregateRootId: String, replyTo: ActorRef[String]) extends DomainCommand

  /** Aggregates that take a while to stop, and record that they did. */
  class SlowStoppingManager(stopped: AtomicBoolean) extends TypedAggregateRootManager[Ping] {
    override def behavior(id: String): Behavior[Ping] =
      Behaviors
        .receiveMessage[Ping] { ping =>
          ping.replyTo ! s"pong $id"
          Behaviors.same
        }
        .receiveSignal {
          case (_, PostStop) =>
            Thread.sleep(300)
            stopped.set(true)
            Behaviors.same
        }

    override def entityTypeKey: EntityTypeKey[Ping] = EntityTypeKey[Ping]("slow-stopping")
  }
}

class TypedGatewayAskAndShutdownSpec
    extends ScalaTestWithActorTestKit(
      ConfigFactory.parseString("""
    pekko.persistence.journal.plugin = "pekko.persistence.journal.inmem"
    pekko.actor.warn-about-java-serializer-usage = off
    pekko.coordinated-shutdown.terminate-actor-system = off
    pekko.coordinated-shutdown.run-by-actor-system-terminate = off
    pekko.coordinated-shutdown.run-by-jvm-shutdown-hook = off
    """)
    )
    with AnyFlatSpecLike
    with LogCapturing {

  import TypedGatewayAskAndShutdownSpec._
  import TypedSimpleAggregate._

  implicit val ec: ExecutionContext    = system.executionContext
  implicit val scheduler: Scheduler    = system.scheduler
  implicit val gatewayTimeout: Timeout = Timeout(5.seconds)
  private val commandMetaData          = AggregateCommandMetaData(OffsetDateTime.now(), None)

  private val acceptAll: ValidateableCommand[SimpleAggregateCommand] =
    new ValidateableCommand[SimpleAggregateCommand] {
      override def validate(cmd: SimpleAggregateCommand): Future[SimpleAggregateCommand] = Future.successful(cmd)
    }

  private val rejectAddItem: ValidateableCommand[SimpleAggregateCommand] =
    new ValidateableCommand[SimpleAggregateCommand] {
      override def validate(cmd: SimpleAggregateCommand): Future[SimpleAggregateCommand] = cmd match {
        case _: AddItem => Future.failed(new IllegalArgumentException("AddItem rejected"))
        case other      => Future.successful(other)
      }
    }

  private val upperCaseItems: ValidateableCommand[SimpleAggregateCommand] =
    new ValidateableCommand[SimpleAggregateCommand] {
      override def validate(cmd: SimpleAggregateCommand): Future[SimpleAggregateCommand] = cmd match {
        case add: AddItem => Future.successful(add.copy(item = add.item.toUpperCase))
        case other        => Future.successful(other)
      }
    }

  behavior of "DefaultTypedCommandGateway.ask"

  it should "not deliver a command that fails validation" in {
    val gateway = new DefaultTypedCommandGateway[SimpleAggregateCommand](system, new SimpleAggregateManager())
    val id      = "ask-rejected"

    gateway.ask[Response](id, Create(id, commandMetaData, _))(acceptAll).futureValue shouldBe OK
    gateway
      .ask[Response](id, AddItem(id, commandMetaData, "one", _))(rejectAddItem)
      .failed
      .futureValue
      .getMessage shouldBe "AddItem rejected"
    gateway.ask[Response](id, GetItems(id, commandMetaData, _))(acceptAll).futureValue shouldBe Items(List.empty)
  }

  it should "deliver the command as the validator returns it" in {
    val gateway = new DefaultTypedCommandGateway[SimpleAggregateCommand](system, new SimpleAggregateManager())
    val id      = "ask-validated"

    gateway.ask[Response](id, Create(id, commandMetaData, _))(upperCaseItems).futureValue shouldBe OK
    gateway.ask[Response](id, AddItem(id, commandMetaData, "one", _))(upperCaseItems).futureValue shouldBe OK
    gateway.ask[Response](id, GetItems(id, commandMetaData, _))(acceptAll).futureValue shouldBe Items(List("ONE"))
  }

  behavior of "DefaultTypedCommandGateway.shutdown"

  it should "complete once its aggregates have stopped" in {
    val stopped = new AtomicBoolean(false)
    val gateway = new DefaultTypedCommandGateway[Ping](system, new SlowStoppingManager(stopped))
    implicit val acceptPing: ValidateableCommand[Ping] = new ValidateableCommand[Ping] {
      override def validate(cmd: Ping): Future[Ping] = Future.successful(cmd)
    }

    gateway.ask[String]("slow", Ping("slow", _)).futureValue shouldBe "pong slow"
    gateway.shutdown().futureValue
    stopped.get shouldBe true

    // a second call completes as well
    gateway.shutdown().futureValue
  }
}
