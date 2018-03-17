/*
 * Copyright (C) 2016-2018 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.cargosample.httpapi

import javax.ws.rs.Path

import akka.actor.ActorSystem
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model.StatusCodes
import akka.event.Logging
import akka.http.scaladsl.common.{
  EntityStreamingSupport,
  JsonEntityStreamingSupport
}
import akka.http.scaladsl.server.{PathMatchers, Route}
import io.cafienne.bounded.aggregate.CommandGateway
import io.cafienne.bounded.cargosample.domain.CargoDomainProtocol.{
  CargoId,
  CargoNotFound
}
import io.cafienne.bounded.cargosample.projections.CargoQueries
import io.cafienne.bounded.cargosample.projections.QueriesJsonProtocol.CargoViewItem
import io.swagger.annotations._
import scala.util.{Failure, Success}

@Path("/")
@Api(value = "cargo",
     produces = "application/json",
     consumes = "application/json")
class CargoRoute(commandGateway: CommandGateway, cargoQueries: CargoQueries)(
    implicit actorSystem: ActorSystem)
    extends SprayJsonSupport {

  import akka.http.scaladsl.server.Directives._
  import HttpJsonProtocol._

  val logger = Logging(actorSystem, CargoRoute.getClass)

  val routes: Route = { getCargo }

  implicit val jsonStreamingSupport: JsonEntityStreamingSupport =
    EntityStreamingSupport.json()

  @Path("cargo/{cargoId}")
  @ApiOperation(value = "Fetch the data of a cargo",
                nickname = "getcargo",
                httpMethod = "GET",
                consumes = "application/json",
                produces = "application/json")
  @ApiImplicitParams(
    Array(
      new ApiImplicitParam(name = "cargoId",
                           paramType = "path",
                           value = "Unique UUID of the cargo",
                           required = true,
                           dataType = "string",
                           example = "c2ea3e36-2ccd-4a20-9d4f-9495d2a170df"),
    ))
  @ApiResponses(
    Array(
      new ApiResponse(code = 200,
                      message = "data of a single cargo",
                      responseContainer = "List",
                      response = classOf[CargoViewItem]),
      new ApiResponse(code = 204, message = "No content"),
      new ApiResponse(code = 500,
                      message = "Internal server error",
                      response = classOf[ErrorResponse])
    ))
  def getCargo =
    get {
      path("cargo" / PathMatchers.JavaUUID) { id =>
        val cargoId = CargoId(id)
        onComplete(cargoQueries.getCargo(cargoId)) {
          case Success(cargoResponse) =>
            complete(StatusCodes.OK -> cargoResponse)
          case Failure(err) => {
            err match {
              case notFound: CargoNotFound =>
                complete(StatusCodes.NotFound -> ErrorResponse(notFound.msg))
              case ex: Throwable =>
                complete(
                  StatusCodes.InternalServerError -> ErrorResponse(
                    ex.getMessage + Option(ex.getCause)
                      .map(t => s" due to ${t.getMessage}")
                      .getOrElse("")))
            }
          }
          case _ => complete(StatusCodes.NoContent)
        }
      }
    }

}

object CargoRoute
