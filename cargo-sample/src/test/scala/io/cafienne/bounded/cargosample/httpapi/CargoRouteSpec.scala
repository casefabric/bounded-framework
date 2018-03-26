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
import io.cafienne.bounded.aggregate._
import io.cafienne.bounded.cargosample.domain.CargoDomainProtocol
import io.cafienne.bounded.cargosample.domain.CargoDomainProtocol._
import io.cafienne.bounded.cargosample.httpapi.HttpJsonProtocol.ErrorResponse
import io.cafienne.bounded.cargosample.projections.{CargoQueries, QueriesJsonProtocol}
import org.scalatest._

import scala.concurrent.Future

class CargoRouteSpec extends FlatSpec with MustMatchers with ScalatestRouteTest {

  import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
  import QueriesJsonProtocol._
  import spray.json._

  val logger = Logging(system, getClass)

  val cargoId1 = CargoId(UUID.fromString("8CD15DA4-006B-478C-8640-2FA52AA7657E"))
  val cargoViewItem1 =
    CargoViewItem(cargoId1, "Amsterdam", "New York", ZonedDateTime.parse("2018-01-01T12:25:38+01:00"))
  val metadata = MetaData(ZonedDateTime.now, None, None)

  val cargoQueries = new CargoQueries {
    override def getCargo(cargoId: CargoDomainProtocol.CargoId): Future[CargoViewItem] = {
      if (cargoId.id.compareTo(cargoId1.id) == 0) {
        Future.successful(cargoViewItem1)
      } else {
        Future.failed[CargoViewItem](CargoNotFound(s"Cargo with id $cargoId is not found"))
      }
    }
  }

  val commandGateway = new CommandGateway {
    override def send[T <: DomainCommand](command: T)(implicit validator: ValidateableCommand[T]): Future[Unit] = ???

    override def sendAndAsk[T <: DomainCommand](command: T)(implicit validator: ValidateableCommand[T]): Future[_] =
      command match {
        case cmd: CargoDomainProtocol.PlanCargo =>
          logger.debug("Received plancargo {}", cmd)
          Future.successful(
            Ok(
              List(
                CargoPlanned(
                  metadata,
                  cargoId1,
                  TrackingId(UUID.fromString("83AB1887-CC3D-434C-855C-34674E746BC0")),
                  RouteSpecification(
                    Location("Amsterdam"),
                    Location("New York"),
                    ZonedDateTime.parse("2018-01-01T13:40:00+01:00")
                  )
                )
              )
            )
          )
        case other =>
          logger.debug("Received other {}", other)
          Future.failed[DomainEvent](new RuntimeException("broken"))
      }
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
    val notExistingCargoId    = CargoId(UUID.fromString("92E597FA-9099-408A-A1D4-5AF7F1A6E761"))
    val expectedErrorResponse = ErrorResponse("Cargo with id 92e597fa-9099-408a-a1d4-5af7f1a6e761 is not found")
    Get(s"/cargo/${notExistingCargoId.id}") ~> Route.seal(cargoRoute.routes) ~> check {
      status must be(StatusCodes.NotFound)
      val theResponse = responseAs[ErrorResponse]
      theResponse must be(expectedErrorResponse)
    }
  }

  it should "send a command to plan the cargo after a post" in {
    val planCargo = HttpJsonProtocol.PlanCargo(
      UUID.fromString("83AB1887-CC3D-434C-855C-34674E746BC0"),
      RouteSpecification(Location("Amsterdam"), Location("New York"), ZonedDateTime.parse("2018-01-01T13:40:00+01:00"))
    )
    Post(s"/cargo", planCargo.toJson) ~> cargoRoute.routes ~> check {
      status must be(StatusCodes.Created)
      val theResponse = responseAs[JsObject]
      theResponse.fields.get("cargoId") must be(Some(JsString("8cd15da4-006b-478c-8640-2fa52aa7657e")))
    }
  }

  it should "give a clear error when the command is unknown" in {
    case class MyHttpCommand(msg: String)
    implicit val MyHttpCommandFmt = jsonFormat1(MyHttpCommand)

    Post(s"/cargo", MyHttpCommand("will not work").toJson) ~> Route.seal(cargoRoute.routes) ~> check {
      status must be(StatusCodes.BadRequest)
      val theResponse = responseAs[String]
      theResponse must be("The request content was malformed:\nObject is missing required member 'trackingId'")
    }
  }

}
