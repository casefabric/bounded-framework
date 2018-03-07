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
package io.cafienne.bounded.cargosample

import java.time.{ZoneOffset, ZonedDateTime}
import java.util.UUID

import akka.actor.ActorSystem
import akka.testkit.TestKit
import akka.util.Timeout
import io.cafienne.bounded.cargosample.aggregate.Cargo.CargoAggregateRootState
import io.cafienne.bounded.cargosample.aggregate.Cargo
import io.cafienne.bounded.cargosample.aggregate.CargoDomainProtocol._
import io.cafienne.bounded.aggregate._
import io.cafienne.bounded.test.TestableAggregateRoot
import org.scalatest._

import scala.concurrent.duration._

class CargoAggregateRootActorNewSpec extends AsyncWordSpec with Matchers with BeforeAndAfterAll {

  implicit val timeout = Timeout(10.seconds) //dilated
  implicit val system = ActorSystem("CargoTestSystem", SpecConfig.testConfigAkkaInMem)

  val userId1 = UserId(UUID.fromString("53f53841-0bf3-467f-98e2-578d360ee572"))
  val userContext = Some(new UserContext {
    override def roles: List[String] = List.empty

    override def userId: UserId = userId1
  })
  val metaData = MetaData(ZonedDateTime.now(ZoneOffset.UTC), userContext, None)

  "CargoAggregateRoot" must {

    "Change the route specification for an existing Cargo Delivery Using AggregateRootTestFixture" in {
      val cargoId3 = CargoId(java.util.UUID.fromString("D31E3C57-E63E-4AD5-A00B-E5FA9196E80D"))
      val trackingId = TrackingId(UUID.fromString("53f53841-0bf3-467f-98e2-578d360ee573"))
      val routeSpecification = RouteSpecification(Location("home"), Location("destination"), ZonedDateTime.parse("2018-03-03T10:15:30+01:00[Europe/Amsterdam]"))
      val cargoPlannedEvent = CargoPlanned(metaData, cargoId3, trackingId, routeSpecification)

      val newRouteSpecification = RouteSpecification(Location("home"), Location("newDestination"),
        ZonedDateTime.parse("2018-03-04T10:45:45+01:00[Europe/Amsterdam]"))
      val specifyNewRouteCommand = SpecifyNewRoute(metaData, cargoId3, newRouteSpecification)

      val ar = TestableAggregateRoot.given[Cargo](cargoId3, cargoPlannedEvent).when(specifyNewRouteCommand)

      // You see that this only shows the events that are 'published' via when
      ar.events should contain (NewRouteSpecified(metaData, cargoId3, newRouteSpecification))

      val targetState = CargoAggregateRootState(trackingId, newRouteSpecification)
      ar.currentState map { state => assert(state == targetState) }
    }

  }

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
 }

}
