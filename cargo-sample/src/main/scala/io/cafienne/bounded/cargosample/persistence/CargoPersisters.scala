/*
 * Copyright (C) 2016-2018 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.cargosample.persistence

import io.cafienne.bounded.akka.persistence.persisters.ForwardsCompatibleSerializer
import io.cafienne.bounded.cargosample.domain.CargoDomainProtocol.{CargoPlanned, NewRouteSpecified}
import stamina.json.persister

object CargoPersisters {

  import io.cafienne.bounded.cargosample.persistence.CargoDomainEventJsonProtocol._

  val v1CargoPlanned = persister[CargoPlanned]("cargoplanned")
  val v1NewRouteSpecified = persister[NewRouteSpecified]("newroutespecified")

  def persisters = List(
    v1CargoPlanned,
    v1NewRouteSpecified
  )
}

class CargoPersistersSerializer extends ForwardsCompatibleSerializer(CargoPersisters.persisters) {}

