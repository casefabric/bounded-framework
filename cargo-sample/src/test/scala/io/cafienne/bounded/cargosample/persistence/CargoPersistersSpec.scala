/*
 * Copyright (C) 2016-2018 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.cargosample.persistence

import java.time.ZonedDateTime
import java.util.UUID

import io.cafienne.bounded.{BuildInfo, RuntimeInfo}
import io.cafienne.bounded.aggregate.{CommandMetaData, MetaData}
import io.cafienne.bounded.cargosample.domain.CargoDomainProtocol._
import org.scalatest._
import stamina.Persisters
import stamina.testkit._

class CargoPersistersSpec extends WordSpecLike with Matchers with StaminaTestKit {

  implicit val buildInfo =
    BuildInfo(io.cafienne.bounded.cargosample.BuildInfo.name, io.cafienne.bounded.cargosample.BuildInfo.version)
  implicit val runtimeInfo = RuntimeInfo("fixed-for-test")

  val persisters = Persisters(CargoPersisters.persisters)

  val userId           = CargoUserId(UUID.fromString("53f53841-0bf3-467f-98e2-578d360ee572"))
  val timestamp        = ZonedDateTime.parse("2018-02-02T10:15:30+01:00")
  val cargoUserContext = CargoUserContext(userId, List.empty)
  val metaData =
    CommandMetaData(timestamp, Some(cargoUserContext), UUID.fromString("60f5b725-799e-423d-8e70-0a664b1e0963"))

  "The Cargo persister" should {
    val cargoId    = CargoId(java.util.UUID.fromString("D31E3C57-E63E-4AD5-A00B-E5FA9196E80D"))
    val trackingId = TrackingId(UUID.fromString("53f53841-0bf3-467f-98e2-578d360ee573"))
    val routeSpecification = RouteSpecification(
      Location("home"),
      Location("destination"),
      ZonedDateTime.parse("2018-03-03T10:15:30+01:00")
    )

    val cargoPlannedEvent = CargoPlanned(MetaData.fromCommand(metaData), cargoId, trackingId, routeSpecification)
    val newRouteSpecified = NewRouteSpecified(MetaData.fromCommand(metaData), cargoId, routeSpecification)

    persisters.generateTestsFor(
      sample(cargoPlannedEvent),
      sample(newRouteSpecified)
    )
  }
}
