// Copyright (C) 2018 the original author or authors.
// See the LICENSE file distributed with this work for additional
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
package io.cafienne.bounded.cargosample.domain

import java.time.{ZoneOffset, ZonedDateTime}
import java.util.UUID

import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import akka.util.Timeout
import io.cafienne.bounded.aggregate._
import io.cafienne.bounded.cargosample.SpecConfig
import io.cafienne.bounded.cargosample.domain.CargoDomainProtocol._
import io.cafienne.bounded.test.CreateEventsInStoreActor
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._

class CargoAggregateRootActorSpec extends TestKit(ActorSystem("CargoTestSystem", SpecConfig.testConfigAkkaInMem))
  with ImplicitSender with WordSpecLike with Matchers with BeforeAndAfterAll {

  implicit val timeout = Timeout(10.seconds) //dilated

  val userId1 = UserId(UUID.fromString("53f53841-0bf3-467f-98e2-578d360ee572"))
  val userContext = Some(new UserContext {
    override def roles: List[String] = List.empty

    override def userId: UserId = userId1
  })
  val metaData = MetaData(ZonedDateTime.now(ZoneOffset.UTC), userContext, None)

  "CargoAggregateRoot" must {

    "Plan a new Cargo Delivery" in {
      val cargoId1 = CargoId(java.util.UUID.fromString("AC935D2D-DD41-4D6C-9302-62C33525B1D2"))
      val trackingId = TrackingId(UUID.fromString("53f53841-0bf3-467f-98e2-578d360ee573"))
      val routeSpecification = RouteSpecification(Location("home"), Location("destination"), ZonedDateTime.parse("2018-03-03T10:15:30+01:00[Europe/Amsterdam]"))
      val planCargoCommand = PlanCargo(metaData, cargoId1, trackingId, routeSpecification)

      val aggregateRootActor = system.actorOf(Cargo.props(cargoId1), "test-aggregate")

      aggregateRootActor ! planCargoCommand

      fishForMessage(10.seconds, hint = "CargoPlanned") {
        case Ok(events) => events.headOption.exists(_.isInstanceOf[CargoPlanned])
      }
    }

    "Change the route specification for an existing Cargo Delivery" in {
      val cargoId2 = CargoId(java.util.UUID.fromString("72DEB9B4-D33F-467E-B1F1-4B0B15D2092F"))
      val trackingId = TrackingId(UUID.fromString("53f53841-0bf3-467f-98e2-578d360ee573"))
      val routeSpecification = RouteSpecification(Location("home"), Location("destination"), ZonedDateTime.parse("2018-03-03T10:15:30+01:00[Europe/Amsterdam]"))
      val cargoPlannedEvent = CargoPlanned(metaData, cargoId2, trackingId, routeSpecification)
      val storeEventsActor = system.actorOf(Props(classOf[CreateEventsInStoreActor], cargoId2), "create-events-actor")

      storeEventsActor ! cargoPlannedEvent
      fishForMessage(10.seconds, "CargoPlanned") {
        case m: CargoPlanned =>
          system.log.debug("Stored CargoPlanned Event for AR actor {}", storeEventsActor)
          true
      }

      val testProbe = TestProbe()
      testProbe watch storeEventsActor

      storeEventsActor ! PoisonPill
      testProbe.expectTerminated(storeEventsActor)

      val aggregateRootActorBackToLife = system.actorOf(Props(classOf[Cargo], cargoId2), "test-aggregate2")

      val newRouteSpecification = RouteSpecification(Location("home"), Location("newDestination"),
        ZonedDateTime.parse("2018-03-04T10:45:45+01:00[Europe/Amsterdam]"))
      val specifyNewRouteCommand = SpecifyNewRoute(metaData, cargoId2, newRouteSpecification)

      // expect this one to have the Planned State
      aggregateRootActorBackToLife ! specifyNewRouteCommand
      fishForMessage(10.seconds, "New Route Specified") {
        case Ok(events) => events.headOption.exists(_.isInstanceOf[NewRouteSpecified])
      }
    }

  }

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
 }

}
