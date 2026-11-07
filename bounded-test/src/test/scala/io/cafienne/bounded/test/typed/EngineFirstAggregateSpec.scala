/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.test.typed

import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.cluster.sharding.typed.scaladsl.EntityTypeKey
import org.apache.pekko.persistence.typed.PersistenceId
import org.apache.pekko.persistence.typed.scaladsl.{Effect, ReplyEffect}
import org.apache.pekko.util.Timeout
import io.cafienne.bounded.aggregate._
import io.cafienne.bounded.aggregate.typed.{AggregateBehavior, TypedAggregateRootManager}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
  * An aggregate that asks another system first and stores its event only when that system accepts ("engine first"),
  * stashing other commands while it waits, tested with `whenAsk` against a fake port.
  */
object EngineFirstAggregateSpec {
  val Type: AggregateType = AggregateType("shipment")

  sealed trait ShipmentCommand                                                                  extends DomainCommand
  final case class Ship(aggregateRootId: String, destination: String, replyTo: ActorRef[Reply]) extends ShipmentCommand
  final case class GetDestinations(aggregateRootId: String, replyTo: ActorRef[List[String]])    extends ShipmentCommand

  // The answer of the port, as a message to the aggregate itself
  private final case class Accepted(aggregateRootId: String, ship: Ship)                extends ShipmentCommand
  private final case class Refused(aggregateRootId: String, ship: Ship, reason: String) extends ShipmentCommand

  final case class Shipped(id: String, destination: String) extends DomainEvent
  final case class CarrierRefused(reason: String)           extends HandlingFailure

  /** The other system: accepts or refuses, after a while. */
  trait CarrierPort {
    def book(destination: String): Future[Boolean]
  }

  class ShipmentManager(port: CarrierPort) extends TypedAggregateRootManager[ShipmentCommand] {
    override def entityTypeKey: EntityTypeKey[ShipmentCommand] = EntityTypeKey[ShipmentCommand](Type.name)

    override def behavior(persistenceId: PersistenceId): Behavior[ShipmentCommand] =
      Behaviors.setup { context =>
        // Not event sourced: only whether the aggregate waits for the port
        var waiting = false

        def handle(state: List[String], command: ShipmentCommand): ReplyEffect[Shipped, List[String]] =
          command match {
            case _ if waiting && !command.isInstanceOf[Accepted] && !command.isInstanceOf[Refused] =>
              Effect.stash()
            case ship: Ship =>
              waiting = true
              context.pipeToSelf(port.book(ship.destination)) {
                case Success(true)  => Accepted(ship.aggregateRootId, ship)
                case Success(false) => Refused(ship.aggregateRootId, ship, "no capacity")
                case Failure(e)     => Refused(ship.aggregateRootId, ship, e.getMessage)
              }
              Effect.noReply
            case Accepted(id, ship) =>
              waiting = false
              val event = Shipped(id, ship.destination)
              Effect
                .persist(event)
                .thenReply(ship.replyTo)((_: List[String]) =>
                  Persisted(List(event), AggregateBehavior.positionOf(context, persistenceId))
                )
                .thenUnstashAll()
            case Refused(_, ship, reason) =>
              waiting = false
              Effect.reply(ship.replyTo)(Ko(CarrierRefused(reason))).thenUnstashAll()
            case GetDestinations(_, replyTo) => Effect.reply(replyTo)(state)
          }

        AggregateBehavior.withEnforcedReplies[ShipmentCommand, Shipped, List[String]](
          persistenceId,
          Nil,
          handle,
          (state, event) => state :+ event.destination
        )
      }
  }

  /** Accepts every destination but "nowhere", after `delay`. */
  def fakePort(delay: FiniteDuration)(implicit ec: ExecutionContext): CarrierPort = new CarrierPort {
    override def book(destination: String): Future[Boolean] =
      Future { Thread.sleep(delay.toMillis); destination != "nowhere" }
  }
}

class EngineFirstAggregateSpec extends AnyWordSpec with Matchers {
  import EngineFirstAggregateSpec._

  implicit val timeout: Timeout     = Timeout(10.seconds)
  implicit val ec: ExecutionContext = ExecutionContext.global

  private val manager = new ShipmentManager(fakePort(300.millis))

  "An engine-first aggregate tested with whenAsk" should {
    "store the event once the port accepts, and reply with its position" in {
      val test = TestableAggregateRoot
        .givenEvents[ShipmentCommand, Shipped, List[String]](manager, "s1")
        .whenAsk(replyTo => Ship("s1", "Rotterdam", replyTo))

      test.reply match {
        case Persisted(events, position) =>
          events shouldBe List(Shipped("s1", "Rotterdam"))
          position.sequenceNr shouldBe 1L
        case other => fail(s"Unexpected $other")
      }
      test.events shouldBe List(Shipped("s1", "Rotterdam"))
      test.shutdown
    }

    "store nothing when the port refuses, and show the refusal as failure" in {
      val test = TestableAggregateRoot
        .givenEvents[ShipmentCommand, Shipped, List[String]](manager, "s2", Shipped("s2", "Hamburg"))
        .whenAsk(replyTo => Ship("s2", "nowhere", replyTo))

      test.reply shouldBe Ko(CarrierRefused("no capacity"))
      test.failure shouldBe CarrierRefused("no capacity")
      test.events shouldBe empty
      test.shutdown
    }

    "stash a command that arrives while it waits, and handle it after the event" in {
      val test = TestableAggregateRoot
        .givenEvents[ShipmentCommand, Shipped, List[String]](manager, "s3")
        .whenAskAll(replyTo => Ship("s3", "Antwerp", replyTo), replyTo => GetDestinations("s3", replyTo))

      test.replies.head shouldBe a[Persisted]
      test.replies(1) shouldBe List("Antwerp")
      test.shutdown
    }

    "refuse a command for another aggregate" in {
      val test = TestableAggregateRoot.givenEvents[ShipmentCommand, Shipped, List[String]](manager, "s4")
      a[TestableAggregateRoot.MisdirectedCommand] should be thrownBy test.whenAsk(replyTo =>
        Ship("other", "x", replyTo)
      )
      test.shutdown
    }
  }
}
