/*
 * Copyright (C) 2016-2018 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.cargosample.domain

import java.util.UUID

import io.cafienne.bounded.cargosample.domain.CargoDomainProtocol._
import spray.json.{RootJsonFormat, _}

object CargoDomainJsonProtocol extends DefaultJsonProtocol {
  import io.cafienne.bounded.aggregate.CommandEventDatastructureJsonProtocol._

  implicit object cargoIdFmt extends RootJsonFormat[CargoId] {
    override def write(obj: CargoId): JsValue = JsString(obj.id.toString)

    override def read(json: JsValue): CargoId = json match {
      case JsString(v) => CargoId(UUID.fromString(v))
      case _ =>
        deserializationError(s"value $json cannot be deserialized to a CargoId")
    }
  }

  implicit object chargeSessionIdFmt extends RootJsonFormat[TrackingId] {
    override def write(obj: TrackingId): JsValue = JsString(obj.id.toString)

    override def read(json: JsValue): TrackingId = json match {
      case JsString(v) => TrackingId(UUID.fromString(v))
      case _ =>
        deserializationError(
          s"value $json cannot be deserialized to a TrackingId")
    }
  }

  implicit val locationFmt = jsonFormat1(Location)
  implicit val routeSpecificationFmt = jsonFormat3(RouteSpecification)

  implicit object CargoNotFoundFmt extends RootJsonFormat[CargoNotFound] {
    override def read(json: JsValue): CargoNotFound =
      json.asJsObject.getFields("message", "cause") match {
        case Seq(JsString(message), JsString(cause)) =>
          CargoNotFound(message, new Throwable(cause))
        case Seq(JsString(message), JsNull) => CargoNotFound(message)
      }

    override def write(obj: CargoNotFound): JsValue =
      JsObject(
        Map("message" -> JsString(obj.msg)).++:(Option(obj.getCause)
          .fold(Map.empty[String, JsValue])(cause =>
            Map("cause" -> JsString(cause.getMessage)))))
  }

  implicit val planCargoFmt = jsonFormat4(PlanCargo)
  implicit val specifyNewRouteFmt = jsonFormat3(SpecifyNewRoute)

}
