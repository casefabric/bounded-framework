/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.aggregate

import org.apache.pekko.actor.{ActorSystem, Props}
import org.apache.pekko.actor.typed.{ActorRef, Behavior, PostStop, Scheduler}
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.scaladsl.adapter._
import org.apache.pekko.cluster.sharding.typed.scaladsl.EntityTypeKey
import org.apache.pekko.persistence.testkit.PersistenceTestKitPlugin
import org.apache.pekko.testkit.TestKit
import org.apache.pekko.util.Timeout
import com.typesafe.config.ConfigFactory
import io.cafienne.bounded.aggregate.typed.{DefaultTypedCommandGateway, TypedAggregateRootManager}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.immutable.Seq
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

/**
  * Tracks per aggregate id how many actors are alive, the most that were alive at once, and how many were
  * started.
  */
object AggregateLifecycles {
  private val alive   = new ConcurrentHashMap[String, AtomicInteger]()
  private val maxSeen = new ConcurrentHashMap[String, AtomicInteger]()
  private val started = new ConcurrentHashMap[String, AtomicInteger]()

  private def counter(map: ConcurrentHashMap[String, AtomicInteger], id: String) =
    map.computeIfAbsent(id, _ => new AtomicInteger())

  def onStart(id: String): Unit = {
    counter(started, id).incrementAndGet()
    val now = counter(alive, id).incrementAndGet()
    counter(maxSeen, id).accumulateAndGet(now, Math.max)
    ()
  }

  def onStop(id: String): Unit = {
    counter(alive, id).decrementAndGet()
    ()
  }

  def maxAlive(id: String): Int = counter(maxSeen, id).get
  def starts(id: String): Int   = counter(started, id).get
}

/** A classic aggregate whose Slow command keeps it busy. */
class SlowClassicAggregate(id: String) extends AggregateRootActor[ClassicSimpleAggregate.ClassicSimpleAggregateState] {
  import ClassicSimpleAggregate._
  import PassivationRaceSpec._

  override def aggregateId: String = id

  override def preStart(): Unit = {
    AggregateLifecycles.onStart(id)
    super.preStart()
  }

  override def postStop(): Unit = {
    AggregateLifecycles.onStop(id)
    super.postStop()
  }

  override def handleCommand(command: DomainCommand, state: Option[ClassicSimpleAggregateState]): Reply =
    command match {
      case Slow(_) =>
        Thread.sleep(BusyFor.toMillis)
        Ok(Seq.empty)
      case Create(aggregateRootId) => Ok(Seq(Created(aggregateRootId)))
      case other                   => Ko(new UnexpectedCommand(other))
    }

  override def newState(evt: DomainEvent): Option[ClassicSimpleAggregateState] =
    Some(ClassicSimpleAggregateState(List.empty))
}

/** An aggregate that is not an AggregateRootActor, so it does not know Passivate. */
class PlainActorAggregate(id: String) extends org.apache.pekko.actor.Actor {
  import ClassicSimpleAggregate._

  override def preStart(): Unit = AggregateLifecycles.onStart(id)

  override def postStop(): Unit = AggregateLifecycles.onStop(id)

  def receive: Receive = {
    case Create(aggregateRootId) => sender() ! Ok(Seq(Created(aggregateRootId)))
  }
}

object PassivationRaceSpec {
  val IdleTime: FiniteDuration = 300.millis
  val BusyFor: FiniteDuration  = 800.millis

  final case class Slow(aggregateRootId: String) extends DomainCommand

  sealed trait TypedCommand                                                    extends DomainCommand
  final case class TypedSlow(aggregateRootId: String, replyTo: ActorRef[String]) extends TypedCommand
  final case class TypedPing(aggregateRootId: String, replyTo: ActorRef[String]) extends TypedCommand

  /** A typed aggregate whose TypedSlow command keeps it busy. */
  object SlowTypedAggregates extends TypedAggregateRootManager[TypedCommand] {
    override def behavior(id: String): Behavior[TypedCommand] = Behaviors.setup { _ =>
      AggregateLifecycles.onStart(id)
      Behaviors
        .receiveMessage[TypedCommand] {
          case TypedSlow(_, replyTo) =>
            Thread.sleep(BusyFor.toMillis)
            replyTo ! "slow done"
            Behaviors.same
          case TypedPing(_, replyTo) =>
            replyTo ! "pong"
            Behaviors.same
        }
        .receiveSignal {
          case (_, PostStop) =>
            AggregateLifecycles.onStop(id)
            Behaviors.same
        }
    }

    override def entityTypeKey: EntityTypeKey[_] = EntityTypeKey[TypedCommand]("slow-typed")
  }
}

/**
  * A command that arrives while an idle aggregate is being stopped must neither start a second actor for it
  * (classic) nor go to the stopping actor and be lost (typed).
  */
class PassivationRaceSpec
    extends TestKit(
      ActorSystem(
        "PassivationRaceSpec",
        PersistenceTestKitPlugin.config.withFallback(ConfigFactory.parseString("""
    pekko.actor.warn-about-java-serializer-usage = off
    pekko.coordinated-shutdown.terminate-actor-system = off
    pekko.coordinated-shutdown.run-by-actor-system-terminate = off
    pekko.coordinated-shutdown.run-by-jvm-shutdown-hook = off
    """))
      )
    )
    with AnyFlatSpecLike
    with ScalaFutures
    with Matchers
    with BeforeAndAfterAll {

  import ClassicSimpleAggregate._
  import PassivationRaceSpec._

  implicit val timeout: Timeout     = Timeout(3.seconds)
  implicit val ec: ExecutionContext = system.dispatcher
  implicit val scheduler: Scheduler = system.toTyped.scheduler
  implicit val patience: PatienceConfig =
    PatienceConfig(timeout = Span(5, Seconds), interval = Span(50, Millis))

  private def valid[T]: ValidateableCommand[T] = new ValidateableCommand[T] {
    override def validate(cmd: T): Future[T] = Future.successful(cmd)
  }
  implicit val slowValidator: ValidateableCommand[Slow]           = valid
  implicit val createValidator: ValidateableCommand[Create]       = valid
  implicit val typedValidator: ValidateableCommand[TypedCommand] = valid

  "RouterCommandGateway" should "hand a command that arrives while the aggregate passivates to a fresh actor" in {
    val aggregateId = "classic-race"
    val creator = new AggregateRootCreator {
      override def props(id: String): Props = Props(classOf[SlowClassicAggregate], id)
    }
    val gateway = new RouterCommandGateway(system, IdleTime, creator)

    // keeps the aggregate busy past its idle time, so it is still running when it is asked to passivate
    val slowReply = gateway.sendAndAsk(Slow(aggregateId))
    Thread.sleep((IdleTime + 200.millis).toMillis)
    val createReply = gateway.sendAndAsk(Create(aggregateId))

    slowReply.futureValue shouldBe Ok(Seq.empty)
    createReply.futureValue shouldBe Ok(Seq(Created(aggregateId)))
    AggregateLifecycles.starts(aggregateId) shouldBe 2
    AggregateLifecycles.maxAlive(aggregateId) shouldBe 1
  }

  it should "stop an idle aggregate that is not an AggregateRootActor without waiting for the force stop" in {
    val aggregateId = "plain-actor"
    val creator = new AggregateRootCreator {
      override def props(id: String): Props = Props(new PlainActorAggregate(id))
    }
    val gateway = new RouterCommandGateway(system, IdleTime, creator)

    gateway.sendAndAsk(Create(aggregateId)).futureValue shouldBe Ok(Seq(Created(aggregateId)))
    Thread.sleep((IdleTime * 3).toMillis)
    AggregateLifecycles.starts(aggregateId) shouldBe 1
    AggregateLifecycles.maxAlive(aggregateId) shouldBe 1

    // the PoisonPill stopped it well within the force stop time, so the next command starts a fresh actor at once
    gateway.sendAndAsk(Create(aggregateId)).futureValue shouldBe Ok(Seq(Created(aggregateId)))
    AggregateLifecycles.starts(aggregateId) shouldBe 2
  }

  "DefaultTypedCommandGateway" should "answer a command that arrives while the aggregate stops from a fresh actor" in {
    val aggregateId = "typed-race"
    val gateway     = new DefaultTypedCommandGateway(system.toTyped, SlowTypedAggregates, IdleTime)

    val slowReply = gateway.ask[String](aggregateId, TypedSlow(aggregateId, _))
    Thread.sleep((IdleTime + 200.millis).toMillis)
    val pingReply = gateway.ask[String](aggregateId, TypedPing(aggregateId, _))

    slowReply.futureValue shouldBe "slow done"
    pingReply.futureValue shouldBe "pong"
    AggregateLifecycles.starts(aggregateId) shouldBe 2
    AggregateLifecycles.maxAlive(aggregateId) shouldBe 1
    gateway.shutdown().futureValue
  }

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system, 30.seconds, verifySystemShutdown = true)
  }
}
