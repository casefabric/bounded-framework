/*
 * Copyright (C) 2016-2019 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.aggregate

import akka.actor.{ActorRef, ActorSystem}
import akka.event.{Logging, LoggingAdapter}
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.{ExecutionContext, Future}

trait CommandGateway {
  def sendAndAsk[T <: DomainCommand](command: T)(implicit validator: ValidateableCommand[T]): Future[_]
  def send[T <: DomainCommand](command: T)(implicit validator: ValidateableCommand[T]): Future[Unit]
}

class DefaultCommandGateway[A <: AggregateRootCreator](system: ActorSystem, aggregateRootCreator: A)(
  implicit timeout: Timeout,
  ec: ExecutionContext
) extends CommandGateway {

  implicit val actorSystem   = system
  val logger: LoggingAdapter = Logging(system, getClass)

  //TODO normal Routee functionality + sleep of actors that were not used for a while
  val aggregateRootInstanceActors =
    collection.mutable.Map[AggregateRootId, ActorRef]()

  private def getAggregateRoot(c: DomainCommand)(implicit system: ActorSystem): ActorRef = {
    aggregateRootInstanceActors.getOrElseUpdate(
      c.aggregateRootId,
      system.actorOf(aggregateRootCreator.props(c.aggregateRootId))
    )
  }

  override def sendAndAsk[T <: DomainCommand](command: T)(implicit validator: ValidateableCommand[T]): Future[_] = {
    CommandValidator
      .validate(command)
      .flatMap(validatedCommand => getAggregateRoot(command) ? validatedCommand)
  }

  override def send[T <: DomainCommand](command: T)(implicit validator: ValidateableCommand[T]): Future[Unit] = {
    CommandValidator
      .validate(command)
      .map(validatedCommand => getAggregateRoot(command) ! validatedCommand)
  }

}
