/*
 * Copyright (C) 2016-2020 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.akka.http

import java.util.UUID

import akka.http.scaladsl.unmarshalling._
import akka.persistence.query.{Offset, Sequence}
import io.cafienne.bounded.config.Configured

/**
  * Actually Deserializers that allow you to get a query parameter (with the parameters directive)
  * and convert it to 'param.as[Offset]
  */
trait AkkaHttpParameterOffsetConverters extends Configured {

  private val configuredJournal =
    config.getString("akka.persistence.journal.plugin")

  implicit val string2OffsetUnmarshaller: FromStringUnmarshaller[Offset] = {
    Unmarshaller.strict[String, Offset] { string ⇒
      try {
        configuredJournal match { //depending on the journal a specific kind of Offset is required, parse according to that.
          case "cassandra-journal" =>
            Offset.timeBasedUUID(UUID.fromString(string))
          case "inmemory-journal" => Offset.sequence(string.toLong)
          case other @ _ =>
            throw new IllegalArgumentException(
              s"value $string cannot be transformed to an offset for journal $configuredJournal"
            )
        }
      } catch {
        case ex: Exception =>
          throw new IllegalArgumentException(
            s"Unmarshalling query parameter $string for $configuredJournal got error " + ex.getMessage
          )
      }
    }
  }
}
