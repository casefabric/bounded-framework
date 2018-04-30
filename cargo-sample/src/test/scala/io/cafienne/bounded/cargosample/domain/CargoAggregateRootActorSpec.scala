/*
 * Copyright (C) 2016-2018 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.cargosample.domain

import java.time.{ZoneOffset, ZonedDateTime}
import java.util.UUID

import akka.actor.ActorSystem
import akka.testkit.TestKit
import akka.util.Timeout
import io.cafienne.bounded.cargosample.domain.Cargo.CargoAggregateState
import io.cafienne.bounded.cargosample.domain.CargoDomainProtocol._
import io.cafienne.bounded.aggregate._
import io.cafienne.bounded.cargosample.SpecConfig
import io.cafienne.bounded.test.TestableAggregateRoot
import org.scalatest._

import scala.concurrent.duration._

class CargoAggregateRootActorSpec extends AsyncWordSpec with Matchers with BeforeAndAfterAll {

  implicit val timeout = Timeout(10.seconds) //dilated
  implicit val system  = ActorSystem("CargoTestSystem", SpecConfig.testConfigAkkaInMem)

  //Creation of Aggregate Roots that make use of dependencies is organized via the Creator
  //as a separate class that contains the required dependencies.
  val cargoAggregateRootCreator = new CargoCreator(system, new FixedLocationsProvider())

  val userId1 = CargoUserId(UUID.fromString("53f53841-0bf3-467f-98e2-578d360ee572"))
  val userContext = Some(new UserContext {
    override def roles: List[String] = List.empty

    override def userId: UserId = userId1
  })
  val metaData = MetaData(ZonedDateTime.now(ZoneOffset.UTC), userContext, None)

  "CargoAggregateRoot" must {

    "Create a new aggregate" in {
      val cargoId2   = CargoId(java.util.UUID.fromString("49A6553D-7E0A-49E8-BE20-925839F524B2"))
      val trackingId = TrackingId(UUID.fromString("53f53841-0bf3-467f-98e2-578d360ee573"))
      val routeSpecification = RouteSpecification(
        Location("home"),
        Location("destination"),
        ZonedDateTime.parse("2018-03-03T10:15:30+01:00[Europe/Amsterdam]")
      )

      val ar = TestableAggregateRoot
        .given[Cargo](cargoAggregateRootCreator, cargoId2)
        .when(PlanCargo(metaData, cargoId2, trackingId, routeSpecification))

      ar.events should contain(CargoPlanned(metaData, cargoId2, trackingId, routeSpecification))
      val targetState = CargoAggregateState(trackingId, routeSpecification)
      ar.currentState map { state =>
        assert(state == targetState)
      }
    }

    "Change the route specification for an existing Cargo Delivery Using AggregateRootTestFixture" in {
      val cargoId3   = CargoId(java.util.UUID.fromString("D31E3C57-E63E-4AD5-A00B-E5FA9196E80D"))
      val trackingId = TrackingId(UUID.fromString("53f53841-0bf3-467f-98e2-578d360ee573"))
      val routeSpecification = RouteSpecification(
        Location("home"),
        Location("destination"),
        ZonedDateTime.parse("2018-03-03T10:15:30+01:00[Europe/Amsterdam]")
      )
      val cargoPlannedEvent = CargoPlanned(metaData, cargoId3, trackingId, routeSpecification)

      val newRouteSpecification = RouteSpecification(
        Location("home"),
        Location("newDestination"),
        ZonedDateTime.parse("2018-03-04T10:45:45+01:00[Europe/Amsterdam]")
      )
      val specifyNewRouteCommand = SpecifyNewRoute(metaData, cargoId3, newRouteSpecification)

      val ar = TestableAggregateRoot
        .given[Cargo](cargoAggregateRootCreator, cargoId3, cargoPlannedEvent)
        .when(specifyNewRouteCommand)

      // You see that this only shows the events that are 'published' via when
      ar.events should contain(NewRouteSpecified(metaData, cargoId3, newRouteSpecification))

      val targetState = CargoAggregateState(trackingId, newRouteSpecification)
      ar.currentState map { state =>
        assert(state == targetState)
      }
    }

  }

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system, 30.seconds, verifySystemShutdown = true)
  }

}
