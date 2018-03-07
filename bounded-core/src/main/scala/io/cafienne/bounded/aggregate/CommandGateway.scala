// Copyright (C) 2018 the original author or authors.
// See the LICENSE file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package io.cafienne.bounded.aggregate

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.{ExecutionContext, Future}

trait CommandGateway {
  def sendAndAsk[T <: AggregateRootCommand](command: T)(implicit validator: ValidateableCommand[T]): Future[_]
  def send[T <: AggregateRootCommand](command: T)(implicit validator: ValidateableCommand[T]): Future[Unit]
}

class DefaultCommandGateway[A <: AggregateRootCreator](system: ActorSystem, aggregateRootCreator: A)
(implicit ec: ExecutionContext, timeout: Timeout) extends CommandGateway {

  implicit val actorSystem = system

  //TODO normal Routee functionality + sleep of actors that were not used for a while
  val aggregateRootInstanceActors = collection.mutable.Map[AggregateRootId, ActorRef]()

  private def getAggregateRoot(c: AggregateRootCommand)(implicit system: ActorSystem): ActorRef = {
    aggregateRootInstanceActors.getOrElseUpdate(c.id, system.actorOf(aggregateRootCreator.create(c.id)))
  }

  override def sendAndAsk[T <: AggregateRootCommand](command: T)(implicit validator: ValidateableCommand[T]): Future[_] = {
    CommandValidator.validate(command).map(validatedCommand => getAggregateRoot(command) ? validatedCommand)
  }

  override def send[T <: AggregateRootCommand](command: T)(implicit validator: ValidateableCommand[T]): Future[Unit] = {
    CommandValidator.validate(command).map(validatedCommand => getAggregateRoot(command) ! validatedCommand)
  }

}