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

  // Aggregates asked to passivate that have not stopped yet, with the commands for them that arrived since.
  private val stopping = collection.mutable.Map[String, Stopping]()

  val aggregateRouter = new AggregateRoutingLogic(routeeMap)
  var router          = Router(aggregateRouter, Vector.empty)

  def receive: Receive = {
    case w: DomainCommand if stopping.contains(w.aggregateRootId) =>
      context.system.log.debug("Buffer message {} until its aggregate has stopped", w)
      val aggregateStopping = stopping(w.aggregateRootId)
      stopping += w.aggregateRootId -> aggregateStopping.copy(buffered = aggregateStopping.buffered :+ (w -> sender()))
    case w: DomainCommand =>
      context.system.log.debug("Route message {}", w)
      route(w, sender())
    case AggregateTerminated(aggregateId, aggregate) =>
      context.system.log.debug("Received Terminated for {}", aggregate)
      removeTerminated(aggregateId, aggregate)
    case StopAggregate(aggregateId) =>
      context.system.log.debug("Received StopAggregate for {}", aggregateId)
      stopAggregate(aggregateId)
    case ForceStop(aggregateId, aggregate) =>
      if (stopping.get(aggregateId).exists(_.aggregate == aggregate)) {
        context.system.log.warning("Aggregate {} did not passivate within {}, stopping it", aggregateId, ForceStopAfter)
        context.stop(aggregate)
      }
  }

  // The aggregate is created when it does not exist, so that it is in the map used by the routing logic.
  private def route(command: DomainCommand, replyTo: ActorRef): Unit = {
    ensureAggregateIsRunning(command.aggregateRootId)
    router.route(command, replyTo)
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

  /**
    * Passivates an idle aggregate. It stops after the commands it already has; commands that arrive before it
    * has stopped are buffered and handed to a fresh actor, so there are never two actors for one aggregate.
    */
  private def stopAggregate(aggregateId: String): Unit = {
    routeeMap.get(aggregateId).foreach { actorRef =>
      timers.cancel(aggregateId)
      router = router.removeRoutee(actorRef)
      routeeMap -= aggregateId
      stopping += aggregateId -> Stopping(actorRef, Vector.empty)
      actorRef ! AggregateRootActor.Passivate
      timers.startSingleTimer(s"force-stop-$aggregateId", ForceStop(aggregateId, actorRef), ForceStopAfter)
    }
  }

  /**
    * An aggregate stopped: after passivating, or by itself (e.g. a failed recovery or persist). Forget it, so
    * the next command for it starts a fresh actor that recovers, and hand that actor the buffered commands.
    */
  private def removeTerminated(aggregateId: String, aggregate: ActorRef): Unit = {
    stopping.get(aggregateId) match {
      case Some(Stopping(stopped, buffered)) if stopped == aggregate =>
        timers.cancel(s"force-stop-$aggregateId")
        stopping -= aggregateId
        buffered.foreach { case (command, replyTo) => route(command, replyTo) }
      case _ if routeeMap.get(aggregateId).contains(aggregate) =>
        timers.cancel(aggregateId)
        router = router.removeRoutee(aggregate)
        routeeMap -= aggregateId
      case _ => ()
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
  private[aggregate] final case class ForceStop(aggregateId: String, aggregate: ActorRef) extends AggregateControl
  private[aggregate] final case class Stopping(aggregate: ActorRef, buffered: Vector[(DomainCommand, ActorRef)])

  // An aggregate that does not stop on Passivate (it is not an AggregateRootActor) is stopped after this time.
  private[aggregate] val ForceStopAfter: FiniteDuration = 10.seconds
}
