/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.aggregate

import org.apache.pekko.actor.{Actor, ActorRef, ActorSystem, Props, Timers}
import org.apache.pekko.event.{Logging, LoggingAdapter}
import org.apache.pekko.routing.{NoRoutee, Routee, RoutingLogic}
import org.apache.pekko.util.Timeout
import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

trait CommandGateway {
  def sendAndAsk[T <: DomainCommand](command: T)(implicit validator: ValidateableCommand[T]): Future[_]
  def send[T <: DomainCommand](command: T)(implicit validator: ValidateableCommand[T]): Future[Unit]
}

/**
  * Kept for compatibility: it now is a RouterCommandGateway that passivates aggregates after 6 idle minutes.
  * It used to keep its aggregates in a map updated from several threads, which could start two actors for one
  * aggregate, never stopped idle aggregates and kept routing to aggregates that had stopped.
  */
@deprecated("Use RouterCommandGateway, which lets you choose the idle time of aggregates", "0.4.0")
class DefaultCommandGateway[A <: AggregateRootCreator](system: ActorSystem, aggregateRootCreator: A)(
  implicit timeout: Timeout,
  ec: ExecutionContext
) extends RouterCommandGateway[A](system, 6.minutes, aggregateRootCreator)

class RouterCommandGateway[A <: AggregateRootCreator](
  system: ActorSystem,
  idleTime: FiniteDuration,
  aggregateRootCreator: A
)(
  implicit timeout: Timeout,
  ec: ExecutionContext
) extends CommandGateway {
  import org.apache.pekko.pattern.ask

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

import org.apache.pekko.routing.{ActorRefRoutee, Router}

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
    case AggregateTerminated(aggregateId, aggregate) =>
      context.system.log.debug("Received Terminated for {}", aggregate)
      removeTerminated(aggregateId, aggregate)
    case StopAggregate(aggregateId) =>
      context.system.log.debug("Received StopAggregate for {}", aggregateId)
      stopAggregate(aggregateId)
  }

  private def ensureAggregateIsRunning(aggregateRootId: String): Unit = {
    routeeMap.getOrElseUpdate(
      aggregateRootId, {
        val aggregate = context.actorOf(aggregateRootCreator.props(aggregateRootId))
        context.watchWith(aggregate, AggregateTerminated(aggregateRootId, aggregate))
        aggregate
      }
    )
    timers.startTimerWithFixedDelay(aggregateRootId, StopAggregate(aggregateRootId), idleTime)
  }

  private def stopAggregate(aggregateId: String): Unit = {
    routeeMap.get(aggregateId).foreach { actorRef =>
      timers.cancel(aggregateId)
      router = router.removeRoutee(actorRef)
      context.stop(actorRef)
    }
    routeeMap -= aggregateId
  }

  /**
    * An aggregate stopped by itself (e.g. a failed recovery or persist) or after StopAggregate: forget it, so
    * the next command for it starts a fresh actor that recovers. When the aggregate was stopped for being idle,
    * a new actor for the same id may already be running; that one is kept.
    */
  private def removeTerminated(aggregateId: String, aggregate: ActorRef): Unit = {
    if (routeeMap.get(aggregateId).contains(aggregate)) {
      timers.cancel(aggregateId)
      router = router.removeRoutee(aggregate)
      routeeMap -= aggregateId
    }
  }
}

private final class AggregateRoutingLogic(routeeMap: collection.mutable.Map[String, ActorRef]) extends RoutingLogic {

  override def select(message: Any, routees: immutable.IndexedSeq[Routee]): Routee = {
    message match {
      case msg: DomainCommand =>
        routeeMap.get(msg.aggregateRootId).fold[Routee](NoRoutee)(foundRef => ActorRefRoutee(foundRef))
      case _ =>
        NoRoutee
    }
  }
}

object AggregateRoutingLogic {
  trait AggregateControl
  final case class StopAggregate(aggregateId: String) extends AggregateControl
  private[aggregate] final case class AggregateTerminated(aggregateId: String, aggregate: ActorRef)
      extends AggregateControl
}
