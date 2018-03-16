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
package io.cafienne.bounded.cargosample.domain

import io.cafienne.bounded.aggregate._
import java.time.ZonedDateTime
import java.util.UUID
import scala.util.control.NoStackTrace

object CargoDomainProtocol {

  case class CargoId(id: UUID) extends AggregateRootId {
    override def idAsString: String = id.toString

    override def toString: String = id.toString
  }

  case class TrackingId(id: UUID)
  case class Location(name: String)
  case class RouteSpecification(origin: Location, destination: Location, arrivalDeadline: ZonedDateTime)

  /**
   * All commands for the Cargo are extended via DomainCommand.
   * This command expects id,  user context and a timestamp as standard input next to to command specific payload.
   *
   * @see DomainCommand for details.
   */
  trait CargoDomainCommand extends DomainCommand {
    override def id: CargoId

    val metaData: MetaData
  }

  /**
   * All events for the Cargo are extended via DomainEvent
   * This event expects id, tenant(id), user context and a timestamp as standard input next to the event specific payload.
   *
   */
  trait CargoDomainEvent extends DomainEvent

  // Commands
  case class PlanCargo(metaData: MetaData, cargoId: CargoId, trackingId: TrackingId, routeSpecification: RouteSpecification) extends CargoDomainCommand {
    override def id: CargoId = cargoId
  }

  case class SpecifyNewRoute(metaData: MetaData, cargoId: CargoId, routeSpecification: RouteSpecification) extends CargoDomainCommand {
    override def id: CargoId = cargoId
  }


  // Events
  case class CargoPlanned(metaData: MetaData, cargoId: CargoId, trackingId: TrackingId, routeSpecification: RouteSpecification) extends CargoDomainEvent {
    override def id: CargoId = cargoId
  }

  case class NewRouteSpecified(metaData: MetaData, CargoId: CargoId, routeSpecification: RouteSpecification) extends CargoDomainEvent {
    override def id: CargoId = CargoId
  }

  trait CargoDomainException extends NoStackTrace {
    val msg: String
  }

  class CargoNotFound(override val msg: String) extends Exception(msg) with CargoDomainException {
    def this(msg: String, cause: Throwable) {
      this(msg)
      initCause(cause)
    }
  }

  object CargoNotFound {
    def apply(msg: String): CargoNotFound =
      new CargoNotFound(msg)
    def apply(msg: String, cause: Throwable): CargoNotFound =
      new CargoNotFound(msg, cause)

    def unapply(e: CargoNotFound): Option[(String, Option[Throwable])] =
      Some((e.getMessage, Option(e.getCause)))
  }

}
