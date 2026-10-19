/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.aggregate.typed

import org.apache.pekko.actor.typed.{ActorRef, ActorSystem, Behavior, PostStop, Scheduler, Terminated}
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import org.apache.pekko.pattern.StatusReply
import org.apache.pekko.util.Timeout
import io.cafienne.bounded.aggregate.{CommandValidator, DomainCommand, ValidateableCommand}

import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

trait TypedCommandGateway[T <: DomainCommand] {
  def ask[Res](aggregateRootId: String, replyTo: ActorRef[Res] => T)(
    implicit validator: ValidateableCommand[T]
  ): Future[Res]
  def tell(command: T)(implicit validator: ValidateableCommand[T]): Future[_]
  def shutdown(): Future[Unit]
}

/**
  * DefaultTypedCommandGateway implements the TypedCommandGateway trait for a LOCAL system
  * it will keep a list of aggregate root actors and use those to ask or tell commands to
  * Typed aggregate roots.
  *
  * TODO: Aggregate Root Actors are responsible for their own lifecycle
  * TODO At this moment, a started AR will not be stopped.
  *
  * @param system A typed actorsystem
  * @param aggregateRootCreator The class that creates the specific type of aggregate root
  * @param aggregateMayIdleFor The time a specific aggregate root may be idle, default is set to 6 minutes
  * @param timeout timeout used for ask
  * @param ec implicit ExecutionContext
  * @param scheduler implicit scheduler for the ask
  * @tparam Cmd The Actual behavior protocol of the Aggregate Root Actor
  */
class DefaultTypedCommandGateway[Cmd <: DomainCommand](
  system: ActorSystem[_],
  aggregateRootCreator: TypedAggregateRootManager[Cmd],
  aggregateMayIdleFor: FiniteDuration = 6.minutes
)(implicit timeout: Timeout, ec: ExecutionContext, scheduler: Scheduler)
    extends TypedCommandGateway[Cmd] {

  import org.apache.pekko.actor.typed.scaladsl.AskPattern._

  object CommandGatewayGuardian {

    sealed trait AggregateControl
    final case class SpawnAggregate(aggregateId: String, replyTo: ActorRef[StatusReply[ActorRef[Cmd]]])
        extends AggregateControl
    final case class StopAggregate(aggregateId: String)                                    extends AggregateControl
    private case class AggregateTerminated(aggregateId: String)                            extends AggregateControl
    final case object GracefulShutdown                                                     extends AggregateControl

    val aggregates = collection.mutable.Map[String, ActorRef[Cmd]]()

    // Aggregates being stopped, with the spawn requests for them that arrived since. Those requests are
    // answered with a fresh actor once the old one has stopped, instead of with the stopping one.
    private val stopping = collection.mutable.Map[String, Vector[ActorRef[StatusReply[ActorRef[Cmd]]]]]()

    def apply(): Behavior[AggregateControl] =
      Behaviors
        .setup[AggregateControl] { context => Behaviors.withTimers { timers => behaviors(timers) } }

    // A failure to create the aggregate fails only the request for it: thrown here, it would stop the guardian and
    // with it every aggregate of this gateway.
    private def findOrCreate(
      context: ActorContext[AggregateControl],
      timers: TimerScheduler[AggregateControl],
      aggregateId: String
    ): StatusReply[ActorRef[Cmd]] = {
      val created = aggregates.get(aggregateId).map(Success(_)).getOrElse {
        Try(context.spawn(aggregateRootCreator.behavior(aggregateId), DefaultTypedCommandGateway.actorName(aggregateId)))
          .map { ref =>
            context.watchWith(ref, AggregateTerminated(aggregateId))
            aggregates += aggregateId -> ref
            ref
          }
      }
      created match {
        case Success(ref) =>
          // Note that the timer will be overwritten, so the last message sent will set the timer.
          timers.startTimerWithFixedDelay(aggregateId, StopAggregate(aggregateId), aggregateMayIdleFor)
          StatusReply.success(ref)
        case Failure(cause) =>
          context.log.error(s"Cannot create aggregate $aggregateId", cause)
          StatusReply.error(cause)
      }
    }

    def behaviors(timers: TimerScheduler[AggregateControl]): Behavior[CommandGatewayGuardian.AggregateControl] =
      Behaviors
        .receive[AggregateControl] { (context, message) =>
          message match {
            case SpawnAggregate(aggregateId, replyTo) if stopping.contains(aggregateId) =>
              context.log.debug("Aggregate {} is stopping, it is created again once it has stopped", aggregateId)
              stopping += aggregateId -> (stopping(aggregateId) :+ replyTo)
              Behaviors.same
            case SpawnAggregate(aggregateId, replyTo) =>
              context.log.debug("Find or create aggregate {}", aggregateId)
              replyTo ! findOrCreate(context, timers, aggregateId)
              Behaviors.same
            case StopAggregate(aggregateId) =>
              context.log.debug("Aggregate {} needs to stop", aggregateId)
              aggregates.get(aggregateId).filterNot(_ => stopping.contains(aggregateId)).foreach { aggregateActorRef =>
                stopping += aggregateId -> Vector.empty
                context.stop(aggregateActorRef)
              }
              Behaviors.same
            case GracefulShutdown =>
              context.log.info("Initiating graceful shutdown...")
              //Stopping the guardian will stop the aggregate root actors.
              Behaviors.stopped { () => context.log.debug("Stopped base on message {}", message) }
            case AggregateTerminated(aggregateId) =>
              context.log.debug("Aggregate {} terminated", aggregateId)
              timers.cancel(aggregateId)
              aggregates.-=(aggregateId)
              stopping.remove(aggregateId).filter(_.nonEmpty).foreach { waiting =>
                val reply = findOrCreate(context, timers, aggregateId)
                waiting.foreach(_ ! reply)
              }
              Behaviors.same
          }
        }
        .receiveSignal {
          case (context, PostStop) =>
            context.log.info("Typed Command Gateway Guardian stopped")
            Behaviors.same
          case (context, Terminated(ref)) =>
            context.log.debug("Terminated actor with ref {}", ref)
            Behaviors.same
        }
  }

  val gateway =
    system.systemActorOf(
      CommandGatewayGuardian(),
      s"typedcommandgateway-${aggregateRootCreator.getClass.getSimpleName}-${DefaultTypedCommandGateway.nextGatewayNumber()}"
    )

  override def ask[Res](aggregateRootId: String, replyTo: ActorRef[Res] => Cmd)(
    implicit validator: ValidateableCommand[Cmd]
  ): Future[Res] = {
    //TODO validation is only possible when the replyTo function is executed and that is in the ask.
    //How to validate the command ?
    spawnAggregateRoot(aggregateRootId).flatMap(ar => {
      ar.ask[Res](replyTo)
    })
  }

  override def tell(command: Cmd)(implicit validator: ValidateableCommand[Cmd]): Future[_] = {
    CommandValidator
      .validate(command)
      .flatMap { validatedCommand =>
        spawnAggregateRoot(validatedCommand.aggregateRootId).map(ar => ar.tell(validatedCommand))
      }
  }

  private def spawnAggregateRoot(aggregateRootId: String): Future[ActorRef[Cmd]] = {
    gateway.askWithStatus(CommandGatewayGuardian.SpawnAggregate(aggregateRootId, _))
  }

  def shutdown(): Future[Unit] = {
    gateway ! CommandGatewayGuardian.GracefulShutdown
    Future.successful((): Unit)
  }

}

object DefaultTypedCommandGateway {
  private val gatewayCount = new AtomicInteger()

  // Gateways for the same manager class get their own guardian name.
  private def nextGatewayNumber(): Int = gatewayCount.incrementAndGet()

  // An aggregate id can hold characters that are not allowed in an actor name, such as '/' or a space, or start with
  // '$'; URL encoding gives every id its own valid name.
  private[bounded] def actorName(aggregateId: String): String =
    URLEncoder.encode(aggregateId, StandardCharsets.UTF_8)
}
