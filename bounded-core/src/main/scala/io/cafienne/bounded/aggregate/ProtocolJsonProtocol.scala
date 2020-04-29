/*
 * Copyright (C) 2016-2020 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.aggregate

import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.UUID

import io.cafienne.bounded._
import io.cafienne.bounded.eventmaterializers.{Compatibility, RuntimeCompatibility}
import spray.json._

object ProtocolJsonProtocol {

  import spray.json.DefaultJsonProtocol._

  implicit val BuildInfoJsonFormat            = jsonFormat2(BuildInfo)
  implicit val RuntimeInfoJsonFormat          = jsonFormat1(RuntimeInfo)
  implicit val RuntimeCompatibilityJsonFormat = jsonEnum(RuntimeCompatibility)
  implicit val CompatibilityJsonFormat        = jsonFormat1(Compatibility)

  def jsonEnum[T <: Enumeration](enu: T): JsonFormat[T#Value] =
    new JsonFormat[T#Value] {
      def write(obj: T#Value): JsValue = JsString(obj.toString)

      def read(json: JsValue): T#Value = json match {
        case JsString(txt) => enu.withName(txt)
        case something =>
          throw DeserializationException(s"Expected a value from enum $enu instead of $something")
      }
    }

  implicit object OffsetDateTimeJsonFormat extends RootJsonFormat[OffsetDateTime] {

    def write(dt: OffsetDateTime): JsValue =
      JsString(
        dt.truncatedTo(ChronoUnit.SECONDS)
          .format(DateTimeFormatter.ISO_DATE_TIME)
      )

    def read(value: JsValue): OffsetDateTime = value match {
      case JsString(v) =>
        OffsetDateTime.parse(v, DateTimeFormatter.ISO_DATE_TIME)
      case _ =>
        deserializationError(s"value $value not conform ISO8601 (yyyy-MM-dd'T'HH:mm:ssZZ) where time is optional")
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

}
