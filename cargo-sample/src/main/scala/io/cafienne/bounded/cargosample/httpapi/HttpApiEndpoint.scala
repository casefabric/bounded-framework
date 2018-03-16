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
package io.cafienne.bounded.cargosample.httpapi

import akka.actor._
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model._
import akka.http.scaladsl.server._
import akka.stream.ActorMaterializer
import io.cafienne.bounded.aggregate.CommandGateway
import io.cafienne.bounded.cargosample.projections.CargoQueries
import io.cafienne.bounded.config.Configured
import spray.json._

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

/**
  * This service wires all provided service routes together and ensures proper handling issues in the requests
  */
class HttpApiEndpoint(commandGateway: CommandGateway, cargoQueries: CargoQueries)(implicit system: ActorSystem, implicit val materializer: ActorMaterializer)
  extends Configured with SprayJsonSupport {

  import Directives._
  import HttpJsonProtocol._

  implicit val ec: ExecutionContext = system.dispatchers.lookup("akka.actor.default-dispatcher")

  val logger = Logging(system, HttpApiEndpoint.getClass)

  def log(e: Throwable)(handle: Route): Route = rc => {
    logger.error(s"Request ${rc.request} could not be handled normally. Cause: ${e.getMessage}", e)
    handle(rc)
  }

  def defaultExceptionHandler(e: Throwable): Route =
    log(e) {
      badRequest(e.getMessage + Option(e.getCause).map(t => s" cause: ${t.getMessage}").getOrElse(""))
    }

  def badRequest(msg: String): Route =
    rc => rc.complete(StatusCodes.BadRequest -> ErrorResponse(msg))

  def notAuthorizedHandler(e: Throwable): Route =
    log(e) {
      rc => rc.complete(StatusCodes.Unauthorized -> ErrorResponse(e.getMessage))
    }

  implicit val requestServiceExceptionHandler = ExceptionHandler {
    //case e: ValidationException => defaultExceptionHandler(e)
    case e: DeserializationException => defaultExceptionHandler(e)
    case e: java.lang.SecurityException => notAuthorizedHandler(e)
    //case e: NotAuthorized => notAuthorizedHandler(e)
    case e: Exception => defaultExceptionHandler(e)
  }

  implicit def requestServiceRejectionHandler =
    RejectionHandler.newBuilder()
      .handle {
        case MalformedRequestContentRejection(errorMessage, cause) => complete(StatusCodes.BadRequest -> ErrorResponse(errorMessage))
      }
      .handle {
        case AuthorizationFailedRejection ⇒ complete(StatusCodes.Forbidden -> ErrorResponse("Not Authorized"))
      }
      .handle {
        case ValidationRejection(msg, _) ⇒ complete(StatusCodes.InternalServerError -> "Internal error due to: " + msg)
      }
      .handleAll[MethodRejection] {
      methodRejections ⇒
        val names = methodRejections.map(_.supported.name)
        complete(StatusCodes.MethodNotAllowed -> ErrorResponse(s"Can't do that! Supported: ${names mkString " or "}!"))
    }
      .handleNotFound { complete(StatusCodes.NotFound -> ErrorResponse("Not here!")) }
      .result()

  // API documentation frontend.
  val swaggerService = new SwaggerHttpServiceRoute(system, materializer)
  val cargoRoute = new CargoRoute(commandGateway, cargoQueries)

  val route = {
    options {
     complete(StatusCodes.OK)
    } ~
    swaggerService.swaggerUIRoute ~
    cargoRoute.routes
  }

  def runServer(host: String, port: Int) = {
    val httpServer = Http().bindAndHandle(route, host, port)

    httpServer onComplete {
      case Success(answer) ⇒ logger.info("service is available: " + answer)
      case Failure(msg) ⇒ logger.error("service failed: " + msg)
    }
  }

}

object HttpApiEndpoint