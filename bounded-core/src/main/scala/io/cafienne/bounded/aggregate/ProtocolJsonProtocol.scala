/*
 * Copyright (C) 2016-2018 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.aggregate

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.UUID

import spray.json._

object ProtocolJsonProtocol extends DefaultJsonProtocol {

  def jsonEnum[T <: Enumeration](enu: T): JsonFormat[T#Value] =
    new JsonFormat[T#Value] {
      def write(obj: T#Value): JsValue = JsString(obj.toString)

      def read(json: JsValue): T#Value = json match {
        case JsString(txt) => enu.withName(txt)
        case something =>
          throw DeserializationException(s"Expected a value from enum $enu instead of $something")
      }
    }

  implicit object ZonedDateTimeJsonFormat extends RootJsonFormat[ZonedDateTime] {

    def write(dt: ZonedDateTime): JsValue =
      JsString(
        dt.truncatedTo(ChronoUnit.SECONDS)
          .toOffsetDateTime
          .format(DateTimeFormatter.ISO_DATE_TIME)
      )

    def read(value: JsValue): ZonedDateTime = value match {
      case JsString(v) =>
        ZonedDateTime.parse(v, DateTimeFormatter.ISO_DATE_TIME)
      case _ =>
        deserializationError(s"value $value not conform ISO8601 (yyyy-MM-dd'T'HH:mm:ssZZ) where time is optional")
    }
  }

  implicit object UserContextJsonFormat extends RootJsonFormat[UserContext] {
    override def write(obj: UserContext): JsValue = JsObject(
      "userId" -> JsString(obj.userId.idAsString),
      "roles"  -> JsArray(obj.roles.map(r => JsString(r)).toVector)
    )

    override def read(json: JsValue): UserContext = json match {
      case JsObject(fields) if fields.contains("userId") =>
        (fields("userId"), fields("roles")) match {
          case (JsString(userStr), JsArray(rolesArr)) =>
            new UserContext {

              override def roles: List[String] =
                rolesArr.map(r => r.toString()).toList

              override def userId: UserId = new UserId {
                override def idAsString: String = userStr
              }
            }
          case _ =>
            deserializationError(s"value $json does not conform the UserContext json object")
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

  implicit val MetaDataJsonFormat = jsonFormat2(MetaData)
}
