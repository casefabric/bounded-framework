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
package io.cafienne.bounded.cargosample.aggregate

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import io.cafienne.bounded.akka.ActorSystemProvider
import io.cafienne.bounded.akka.persistence.ReadJournalProvider
import io.cafienne.bounded.commands.ValidateableCommand
import io.cafienne.bounded.cargosample.aggregate.CargoDomainProtocol.{CargoDomainCommand, CargoNotFound, PlanCargo, SpecifyNewRoute}

import scala.concurrent.Future

trait CargoCommandValidators {
  implicit val PlanCargoValidator: ValidateableCommand[PlanCargo]
  implicit val SpecifyNewRouteValidator: ValidateableCommand[SpecifyNewRoute]
}

trait ExistenceChecker extends ActorSystemProvider with ReadJournalProvider {
  implicit val materializer = ActorMaterializer()
  implicit val ec = system.dispatcher

  def exists[A <: CargoDomainCommand](cmd: A): Future[A] =
    readJournal.currentEventsByPersistenceId(cmd.id.idAsString, 0, 1)
      .runFold(false)((_, _) => true)
      .flatMap(f => if (f) Future.successful(cmd) else Future.failed(CargoNotFound(s"Cargo with id ${cmd.id} not found while processing command $cmd")))
}

class CargoCommandValidatorsImpl(actorSystem: ActorSystem) extends CargoCommandValidators {

  implicit val PlanCargoValidator = new PlanCargoValidator()
  implicit val SpecifyNewRouteValidator = new CargoCommandValidator[SpecifyNewRoute](actorSystem)

}

class PlanCargoValidator() extends ValidateableCommand[PlanCargo] {
  override def validate(cmd: PlanCargo): Future[PlanCargo] = {
    Future.successful(cmd)
  }
}

class CargoCommandValidator[T <: CargoDomainCommand](actorSystem: ActorSystem) extends ValidateableCommand[T] with ExistenceChecker {
  override implicit def system: ActorSystem = actorSystem

  override def validate(cmd: T): Future[T] = exists[T](cmd)
}
