/*
 * Copyright (C) 2016-2018 Cafienne B.V. <https://www.cafienne.io/bounded>
 */
package io.cafienne.bounded.cargosample.httpapi

import java.time.ZonedDateTime
import java.util.UUID

import akka.event.Logging
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import io.cafienne.bounded.aggregate.{DomainCommand, CommandGateway, ValidateableCommand}
import io.cafienne.bounded.cargosample.domain.CargoDomainProtocol
import io.cafienne.bounded.cargosample.domain.CargoDomainProtocol.{CargoId, CargoNotFound}
import io.cafienne.bounded.cargosample.httpapi.HttpJsonProtocol.ErrorResponse
import io.cafienne.bounded.cargosample.projections.{CargoQueries, QueriesJsonProtocol}
import org.scalatest._

import scala.concurrent.Future

class CargoRouteSpec extends FlatSpec with MustMatchers with ScalatestRouteTest {

  import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
  import QueriesJsonProtocol._

  val logger = Logging(system, getClass)

  val cargoId1 = CargoId(UUID.fromString("8CD15DA4-006B-478C-8640-2FA52AA7657E"))
  val cargoViewItem1 = CargoViewItem(cargoId1, "Amsterdam", "New York", ZonedDateTime.parse("2018-01-01T12:25:38+01:00"))

  val cargoQueries = new CargoQueries {
    override def getCargo(cargoId: CargoDomainProtocol.CargoId): Future[CargoViewItem] = {
      if (cargoId.id.compareTo(cargoId1.id) == 0) {
        Future.successful(cargoViewItem1)
      } else {
        Future.failed[CargoViewItem](CargoNotFound(s"Cargo with id $cargoId is not found"))
      }
    }
  }

  //TODO make use of testing command gateway ?
  val commandGateway = new CommandGateway {
    override def send[T <: DomainCommand](command: T)(implicit validator: ValidateableCommand[T]): Future[Unit] = ???

    override def sendAndAsk[T <: DomainCommand](command: T)(implicit validator: ValidateableCommand[T]): Future[_] = ???
  }

  val cargoRoute = new CargoRoute(commandGateway, cargoQueries)

  "The Cargo route" should "fetch the data of a specific piece of cargo" in {
    Get(s"/cargo/${cargoId1.id}") ~> Route.seal(cargoRoute.routes) ~> check {
      status must be(StatusCodes.OK)
      val theResponse = responseAs[CargoViewItem]
      theResponse must be(cargoViewItem1)
    }
  }

  it should "respond with a not found when the cargo does not exist" in {
    val notExistingCargoId = CargoId(UUID.fromString("92E597FA-9099-408A-A1D4-5AF7F1A6E761"))
    val expectedErrorResponse = ErrorResponse("Cargo with id 92e597fa-9099-408a-a1d4-5af7f1a6e761 is not found")
    Get(s"/cargo/${notExistingCargoId.id}") ~> Route.seal(cargoRoute.routes) ~> check {
      status must be(StatusCodes.NotFound)
      val theResponse = responseAs[ErrorResponse]
      theResponse must be(expectedErrorResponse)
    }

  }

}