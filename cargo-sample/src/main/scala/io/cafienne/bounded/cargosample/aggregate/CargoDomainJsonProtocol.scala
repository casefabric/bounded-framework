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

import java.util.UUID

import io.cafienne.bounded.cargosample.aggregate.CargoDomainProtocol._
import spray.json.{RootJsonFormat, _}

object CargoDomainJsonProtocol extends DefaultJsonProtocol {
  import io.cafienne.bounded.aggregate.CommandEventDatastructureJsonProtocol._

  implicit object smartDriverIdFmt extends RootJsonFormat[CargoId] {
    override def write(obj: CargoId): JsValue = JsString(obj.id.toString)

    override def read(json: JsValue): CargoId = json match {
      case JsString(v) => CargoId(UUID.fromString(v))
      case _ => deserializationError(s"value $json cannot be deserialized to a CargoId")
    }
  }

  implicit object chargeSessionIdFmt extends RootJsonFormat[TrackingId] {
    override def write(obj: TrackingId): JsValue = JsString(obj.id.toString)

    override def read(json: JsValue): TrackingId = json match {
      case JsString(v) => TrackingId(UUID.fromString(v))
      case _ => deserializationError(s"value $json cannot be deserialized to a TrackingId")
    }
  }

  implicit val locationFmt = jsonFormat1(Location)
  implicit val routeSpecificationFmt = jsonFormat3(RouteSpecification)

  implicit object CargoNotFoundFmt extends RootJsonFormat[CargoNotFound] {
    override def read(json: JsValue): CargoNotFound = json.asJsObject.getFields("message", "cause") match {
      case Seq(JsString(message), JsString(cause)) => CargoNotFound(message, new Throwable(cause))
      case Seq(JsString(message), JsNull) => CargoNotFound(message)
    }

    override def write(obj: CargoNotFound): JsValue = JsObject(Map("message" -> JsString(obj.msg)).++:(Option(obj.getCause)
      .fold(Map.empty[String,JsValue])(cause => Map("cause" -> JsString(cause.getMessage)))))
  }

  implicit val planCargoFmt = jsonFormat4(PlanCargo)
  implicit val specifyNewRouteFmt = jsonFormat3(SpecifyNewRoute)

}
