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
package bounded.akka.http

import java.util.UUID

import akka.http.scaladsl.unmarshalling._
import akka.persistence.query.{Offset, Sequence}
import bounded.config.Configured

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
          case "inmemory-journal" => Sequence.apply(string.toLong)
          case other =>
            throw new IllegalArgumentException(
              s"value $string cannot be transformed to an offset for journal $configuredJournal")
        }
      } catch {
        case ex: Exception =>
          throw new IllegalArgumentException(
            s"Unmarshalling query parameter $string for $configuredJournal got error " + ex.getMessage)
      }
    }
  }
}
