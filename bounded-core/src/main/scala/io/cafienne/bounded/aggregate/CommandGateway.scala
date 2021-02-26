/*
 * Copyright (C) 2016-2021 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.aggregate

import akka.actor.{Actor, ActorContext, ActorRef, ActorSystem, Props, Terminated, TimerScheduler, Timers}
import akka.event.{Logging, LoggingAdapter}
import akka.routing.{NoRoutee, Routee, RoutingLogic}
import akka.util.Timeout
import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

trait CommandGateway {
  def sendAndAsk[T <: DomainCommand](command: T)(implicit validator: ValidateableCommand[T]): Future[_]
  def send[T <: DomainCommand](command: T)(implicit validator: ValidateableCommand[T]): Future[Unit]
}

class DefaultCommandGateway[A <: AggregateRootCreator](system: ActorSystem, aggregateRootCreator: A)(
  implicit timeout: Timeout,
  ec: ExecutionContext
) extends CommandGateway {
  import akka.pattern.ask

  implicit val actorSystem   = system
  val logger: LoggingAdapter = Logging(system, getClass)

  //TODO normal Routee functionality + sleep of actors that were not used for a while
  val aggregateRootInstanceActors =
    collection.mutable.Map[String, ActorRef]()

  private def getAggregateRoot(c: DomainCommand)(implicit system: ActorSystem): ActorRef = {
    aggregateRootInstanceActors.getOrElseUpdate(
      c.aggregateRootId,
      system.actorOf(aggregateRootCreator.props(c.aggregateRootId))
    )
  }

  override def sendAndAsk[T <: DomainCommand](command: T)(implicit validator: ValidateableCommand[T]): Future[_] = {
//    getAggregateRoot(command).ask(command)
    CommandValidator
      .validate(command)
      .flatMap(validatedCommand => {
        val foundAr = getAggregateRoot(command)
        logger.debug("sendAndAsk cmd {} to {} ", validatedCommand.getClass.getSimpleName, foundAr)
        foundAr ? validatedCommand
      })
  }

  override def send[T <: DomainCommand](command: T)(implicit validator: ValidateableCommand[T]): Future[Unit] = {
    CommandValidator
      .validate(command)
      .map(validatedCommand => {
        val foundAr = getAggregateRoot(command)
        logger.debug("send cmd {} to {} ", validatedCommand.getClass.getSimpleName, foundAr)
        foundAr ! validatedCommand
      })
  }

}

class RouterCommandGateway[A <: AggregateRootCreator](
  system: ActorSystem,
  idleTime: FiniteDuration,
  aggregateRootCreator: A
)(
  implicit timeout: Timeout,
  ec: ExecutionContext
) extends CommandGateway {
  import akka.pattern.ask

  implicit val actorSystem: ActorSystem = system
  val logger: LoggingAdapter            = Logging(system, getClass)

  val gatewayActor = actorSystem.actorOf(Props(new AggregateGatewayRouter(aggregateRootCreator, idleTime)))

  override def sendAndAsk[T <: DomainCommand](command: T)(implicit validator: ValidateableCommand[T]): Future[_] = {
    for {
      validatedCommand <- CommandValidator.validate(command)
      answer           <- gatewayActor ? validatedCommand
    } yield answer
  }

  override def send[T <: DomainCommand](command: T)(implicit validator: ValidateableCommand[T]): Future[Unit] = {
    CommandValidator
      .validate(command)
      .map(validatedCommand => gatewayActor ! validatedCommand)
  }

}

import akka.routing.{ActorRefRoutee, Router}

private class AggregateGatewayRouter[A <: AggregateRootCreator](aggregateRootCreator: A, idleTime: FiniteDuration)
    extends Actor
    with Timers {
  import AggregateRoutingLogic._
  val routeeMap = collection.mutable.Map[String, ActorRef]()

  val aggregateRouter = new AggregateRoutingLogic(routeeMap)
  var router          = Router(aggregateRouter, Vector.empty)

  def receive: Receive = {
    case w: DomainCommand =>
      context.system.log.debug("Route message {}", w)
      //Aggregate is created when it does not exist, required to ensure that it will be in the map used
      //in the routing logic.
      ensureAggregateIsRunning(w.aggregateRootId)
      router.route(w, sender())
    case Terminated(a) =>
      context.system.log.debug("Received Terminated for {}", a)
    case StopAggregate(aggregateId) =>
      context.system.log.debug("Received StopAggregate for {}", aggregateId)
      stopAggregate(aggregateId)
  }

  private def ensureAggregateIsRunning(aggregateRootId: String): Unit = {
    val foundAggregate = routeeMap.getOrElseUpdate(
      aggregateRootId,
      context.actorOf(aggregateRootCreator.props(aggregateRootId))
    )
    context.watch(foundAggregate)
    timers.startTimerWithFixedDelay(aggregateRootId, StopAggregate(aggregateRootId), idleTime)
  }

  private def stopAggregate(aggregateId: String): Unit = {
    routeeMap.get(aggregateId).foreach { actorRef =>
      timers.cancel(aggregateId)
      router = router.removeRoutee(actorRef)
      context.stop(actorRef)
    }
    routeeMap.remove(aggregateId)
  }
}

private final class AggregateRoutingLogic(routeeMap: collection.mutable.Map[String, ActorRef]) extends RoutingLogic {

  override def select(message: Any, routees: immutable.IndexedSeq[Routee]): Routee = {
    message match {
      case msg: DomainCommand =>
        routeeMap.get(msg.aggregateRootId).fold[Routee](NoRoutee)(foundRef => ActorRefRoutee(foundRef))
      case other =>
        NoRoutee
    }
  }
}

object AggregateRoutingLogic {
  trait AggregateControl
  final case class StopAggregate(aggregateId: String) extends AggregateControl
}
