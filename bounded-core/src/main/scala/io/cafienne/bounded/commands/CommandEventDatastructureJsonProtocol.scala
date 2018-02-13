// Copyright (C) 2018 the original author or authors.
// See the LICENCE.txt file distributed with this work for additional
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
package io.cafienne.bounded.commands

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.UUID

import io.cafienne.bounded.akka.persistence.eventmaterializers.EventNumber
import spray.json._

object CommandEventDatastructureJsonProtocol extends DefaultJsonProtocol {

  def jsonEnum[T <: Enumeration](enu: T): JsonFormat[T#Value] =
    new JsonFormat[T#Value] {
      def write(obj: T#Value): JsValue = JsString(obj.toString)

      def read(json: JsValue): T#Value = json match {
        case JsString(txt) => enu.withName(txt)
        case something =>
          throw DeserializationException(
            s"Expected a value from enum $enu instead of $something")
      }
    }

  implicit object ZonedDateTimeJsonFormat
      extends RootJsonFormat[ZonedDateTime] {

    def write(dt: ZonedDateTime): JsValue =
      JsString(
        dt.truncatedTo(ChronoUnit.SECONDS)
          .toOffsetDateTime
          .format(DateTimeFormatter.ISO_DATE_TIME))

    def read(value: JsValue): ZonedDateTime = value match {
      case JsString(v) =>
        ZonedDateTime.parse(v, DateTimeFormatter.ISO_DATE_TIME)
      case _ =>
        deserializationError(
          s"value $value not conform ISO8601 (yyyy-MM-dd'T'HH:mm:ssZZ) where time is optional")
    }
  }

  implicit object UserContextJsonFormat extends RootJsonFormat[UserContext] {
    override def write(obj: UserContext): JsValue = JsObject(
      "userId" -> JsString(obj.userId.idAsString),
      "roles" -> JsArray(obj.roles.map(r => JsString(r)).toVector)
    )

    override def read(json: JsValue): UserContext = json match {
      case JsObject(fields) if fields.contains("userId") =>
        (fields("userId"), fields("roles")) match {
          case (JsString(userStr), JsArray(rolesArr)) =>
            new UserContext {

              override def roles: List[String] =
                rolesArr.map(r => r.toString()).toList

              override def userId: UserId = UserId(UUID.fromString(userStr))
            }
          case _ =>
            deserializationError(
              s"value $json does not conform the UserContext json object")
        }
    }
  }

  implicit object JavaUUIDFormat extends RootJsonFormat[UUID] {
    override def write(obj: UUID): JsValue = JsString(obj.toString)

    override def read(json: JsValue): UUID = json match {
      case JsString(v) => UUID.fromString(v)
      case _ =>
        deserializationError(s"value $json cannot be deserialized to a UUID")
    }
  }

  implicit val EventNumberFormat = jsonFormat1(EventNumber)
  implicit val MetaDataJsonFormat = jsonFormat3(MetaData)
}
