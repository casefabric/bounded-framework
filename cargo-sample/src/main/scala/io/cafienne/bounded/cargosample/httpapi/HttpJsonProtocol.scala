/*
 * Copyright (C) 2016-2018 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.cargosample.httpapi

import spray.json._

/**
  * JSON protocol used by the http API.
  * Contains specific protocol messages and the JSON serialization instructions for the Spray JSON format
  * for all classes used with the http API.
  */
object HttpJsonProtocol extends DefaultJsonProtocol {

  case class ErrorResponse(msg: String)

  implicit val errorResponsFmt = jsonFormat1(ErrorResponse)

}
