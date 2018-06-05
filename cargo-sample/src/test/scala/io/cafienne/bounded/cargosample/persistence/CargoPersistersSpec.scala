/*
 * Copyright (C) 2016-2018 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.cargosample.persistence

import java.time.ZonedDateTime
import java.util.UUID

import io.cafienne.bounded.aggregate.MetaData
import io.cafienne.bounded.cargosample.domain.CargoDomainProtocol._
import org.scalatest._
import stamina.Persisters
import stamina.testkit._

class CargoPersistersSpec extends WordSpecLike with Matchers with StaminaTestKit {

  val persisters = Persisters(CargoPersisters.persisters)

  val userId           = CargoUserId(UUID.fromString("53f53841-0bf3-467f-98e2-578d360ee572"))
  val timestamp        = ZonedDateTime.parse("2018-02-02T10:15:30+01:00")
  val cargoUserContext = CargoUserContext(userId, List.empty)
  val metaData         = MetaData(timestamp, Some(cargoUserContext))

  "The Cargo persister" should {
    val cargoId    = CargoId(java.util.UUID.fromString("D31E3C57-E63E-4AD5-A00B-E5FA9196E80D"))
    val trackingId = TrackingId(UUID.fromString("53f53841-0bf3-467f-98e2-578d360ee573"))
    val routeSpecification = RouteSpecification(
      Location("home"),
      Location("destination"),
      ZonedDateTime.parse("2018-03-03T10:15:30+01:00")
    )

    val cargoPlannedEvent = CargoPlanned(metaData, cargoId, trackingId, routeSpecification)
    val newRouteSpecified = NewRouteSpecified(metaData, cargoId, routeSpecification)

    persisters.generateTestsFor(
      sample(cargoPlannedEvent),
      sample(newRouteSpecified)
    )
  }
}
