/*
 * Copyright (C) 2016-2018 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.cargosample.httpapi

import java.util.UUID

import io.cafienne.bounded.cargosample.domain.CargoDomainProtocol.RouteSpecification
import spray.json._

/**
  * JSON protocol used by the http API.
  * Contains specific protocol messages and the JSON serialization instructions for the Spray JSON format
  * for all classes used with the http API.
  */
object HttpJsonProtocol extends DefaultJsonProtocol {
  import io.cafienne.bounded.aggregate.ProtocolJsonProtocol._
  import io.cafienne.bounded.cargosample.domain.CargoDomainJsonProtocol._

  case class PlanCargo(trackingId: UUID, routeSpecification: RouteSpecification)

  implicit val planCargoFmt = jsonFormat2(PlanCargo)

  case class ErrorResponse(msg: String)

  implicit val errorResponsFmt = jsonFormat1(ErrorResponse)

}
