/*
 * Copyright (C) 2016-2021 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.aggregate.typed

import akka.actor.typed.{ActorRef, ActorSystem, Behavior, PostStop, Scheduler, Terminated}
import akka.actor.typed.scaladsl.Behaviors
import akka.util.Timeout
import io.cafienne.bounded.aggregate.{CommandValidator, DomainCommand, ValidateableCommand}

import scala.concurrent.{ExecutionContext, Future}

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
  * @param timeout timeout used for ask
  * @param ec implicit ExecutionContext
  * @param scheduler implicit scheduler for the ask
  * @tparam Cmd The Actual behavior protocol of the Aggregate Root Actor
  */
class DefaultTypedCommandGateway[Cmd <: DomainCommand](
  system: ActorSystem[_],
  aggregateRootCreator: TypedAggregateRootManager[Cmd]
)(implicit timeout: Timeout, ec: ExecutionContext, scheduler: Scheduler)
    extends TypedCommandGateway[Cmd] {

  import akka.actor.typed.scaladsl.AskPattern._

  object CommandGatewayGuardian {

    sealed trait AggregateControl
    final case class SpawnAggregate(aggregateId: String, replyTo: ActorRef[ActorRef[Cmd]]) extends AggregateControl
    final case class StopAggregate(aggregateId: String)                                    extends AggregateControl
    private case class AggregateTerminated(aggregateId: String)                            extends AggregateControl
    final case object GracefulShutdown                                                     extends AggregateControl

    val aggregates = collection.mutable.Map[String, ActorRef[Cmd]]()

    def apply(): Behavior[AggregateControl] =
      Behaviors
        .setup[AggregateControl] { context => behaviors }

    val behaviors: Behavior[CommandGatewayGuardian.AggregateControl] = Behaviors
      .receive[AggregateControl] { (context, message) =>
        message match {
          case SpawnAggregate(aggregateId, replyTo) =>
            context.log.debug("Find or create aggregate {}", aggregateId)
            val ref = aggregates.getOrElseUpdate(
              aggregateId, {
                val ref = context.spawn(aggregateRootCreator.behavior(aggregateId), name = aggregateId)
                context.watchWith(ref, AggregateTerminated(aggregateId))
                ref
              }
            )
            replyTo ! ref
            Behaviors.same
          case StopAggregate(aggregateId) =>
            context.log.debug("Aggregate {} needs to stop", aggregateId)
            aggregates.get(aggregateId).foreach(aggregateActorRef => context.stop(aggregateActorRef))
            Behaviors.same
          case GracefulShutdown =>
            context.log.info("Initiating graceful shutdown...")
            //Stopping the guardian will stop the aggregate root actors.
            Behaviors.stopped { () => context.log.debug("Stopped base on message {}", message) }
          case AggregateTerminated(aggregateId) =>
            context.log.debug("Aggregate {} terminated", aggregateId)
            aggregates.-=(aggregateId)
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

  val gateway = system.systemActorOf(CommandGatewayGuardian(), "typedcommandgateway")

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
    gateway.ask(CommandGatewayGuardian.SpawnAggregate(aggregateRootId, _))
  }

  def shutdown(): Future[Unit] = {
    gateway ! CommandGatewayGuardian.GracefulShutdown
    Future.successful((): Unit)
  }

}
