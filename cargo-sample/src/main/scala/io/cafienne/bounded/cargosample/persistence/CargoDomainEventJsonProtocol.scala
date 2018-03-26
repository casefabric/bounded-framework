/*
 * Copyright (C) 2016-2018 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.cargosample.persistence

import io.cafienne.bounded.cargosample.domain.CargoDomainProtocol.{CargoPlanned, NewRouteSpecified}
import spray.json._

object CargoDomainEventJsonProtocol extends DefaultJsonProtocol {
  import io.cafienne.bounded.aggregate.ProtocolJsonProtocol._
  import io.cafienne.bounded.cargosample.domain.CargoDomainJsonProtocol._

  implicit val cargoPlannedFmt      = jsonFormat4(CargoPlanned)
  implicit val newRouteSpecifiedFmt = jsonFormat3(NewRouteSpecified)

}
