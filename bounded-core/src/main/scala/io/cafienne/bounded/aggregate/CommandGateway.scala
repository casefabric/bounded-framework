/*
 * Copyright (C) 2016-2018 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.aggregate

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.{ExecutionContext, Future}

trait CommandGateway {
  def sendAndAsk[T <: DomainCommand](command: T)(implicit validator: ValidateableCommand[T]): Future[_]
  def send[T <: DomainCommand](command: T)(implicit validator: ValidateableCommand[T]): Future[Unit]
}

class DefaultCommandGateway[A <: AggregateRootCreator](system: ActorSystem, aggregateRootCreator: A)
(implicit ec: ExecutionContext, timeout: Timeout) extends CommandGateway {

  implicit val actorSystem = system

  //TODO normal Routee functionality + sleep of actors that were not used for a while
  val aggregateRootInstanceActors = collection.mutable.Map[AggregateRootId, ActorRef]()

  private def getAggregateRoot(c: DomainCommand)(implicit system: ActorSystem): ActorRef = {
    aggregateRootInstanceActors.getOrElseUpdate(
      c.id, system.actorOf(aggregateRootCreator.create(c.id)))
  }

  override def sendAndAsk[T <: DomainCommand](command: T)(implicit validator: ValidateableCommand[T]): Future[_] = {
    CommandValidator.validate(command).map(validatedCommand => getAggregateRoot(command) ? validatedCommand)
  }

  override def send[T <: DomainCommand](command: T)(implicit validator: ValidateableCommand[T]): Future[Unit] = {
    CommandValidator.validate(command).map(validatedCommand => getAggregateRoot(command) ! validatedCommand)
  }

}
