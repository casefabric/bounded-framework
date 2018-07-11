/*
 * Copyright (C) 2016-2018 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.cargosample.domain

import io.cafienne.bounded.aggregate._
import java.time.ZonedDateTime
import java.util.UUID

import io.cafienne.bounded.{UserContext, UserId}

import scala.util.control.NoStackTrace

object CargoDomainProtocol {

  case class CargoUserId(id: UUID) extends UserId {
    override def idAsString: String = id.toString
  }

  case class CargoId(id: UUID) extends AggregateRootId {
    override def idAsString: String = id.toString

    override def toString: String = id.toString
  }

  case class CargoUserContext(userId: UserId, roles: List[String]) extends UserContext

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
    override def aggregateRootId: CargoId

    val metaData: CommandMetaData
  }

  /**
    * All events for the Cargo are extended via DomainEvent
    * This event expects id, tenant(id), user context and a timestamp as standard input next to the event specific payload.
    *
    */
  trait CargoDomainEvent extends DomainEvent

  // Commands
  case class PlanCargo(
    metaData: CommandMetaData,
    cargoId: CargoId,
    trackingId: TrackingId,
    routeSpecification: RouteSpecification
  ) extends CargoDomainCommand {
    override def aggregateRootId: CargoId = cargoId
  }

  case class SpecifyNewRoute(metaData: CommandMetaData, cargoId: CargoId, routeSpecification: RouteSpecification)
      extends CargoDomainCommand {
    override def aggregateRootId: CargoId = cargoId
  }

  // Events
  case class CargoPlanned(
    metaData: MetaData,
    cargoId: CargoId,
    trackingId: TrackingId,
    routeSpecification: RouteSpecification
  ) extends CargoDomainEvent {
    override def id: CargoId = cargoId
  }

  case class NewRouteSpecified(metaData: MetaData, CargoId: CargoId, routeSpecification: RouteSpecification)
      extends CargoDomainEvent {
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
