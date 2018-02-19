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

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import akka.util.Timeout
import io.cafienne.bounded.cargosample.SpecConfig._
import io.cafienne.bounded.cargosample.aggregate.CargoDomainProtocol._
import io.cafienne.bounded.cargosample.aggregate.{Cargo, CargoAggregateRootRouterProvider}
import io.cafienne.bounded.commands.{CommandNotProcessedException, MetaData, UserContext, UserId}
import io.cafienne.bounded.test.StopSystemAfterAll
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.duration._
import scala.util.Right

class CargoAggregateRootSpec extends TestKit(ActorSystem("testsystem", testConfig)) with ImplicitSender
    with WordSpecLike with Matchers with StopSystemAfterAll with CargoAggregateRootRouterProvider {

//  implicit val routeTestTimeout = RouteTestTimeout(5.seconds)

  val cargoId1 = CargoId(java.util.UUID.fromString("AC935D2D-DD41-4D6C-9302-62C33525B1D2"))
  val trackingId = TrackingId(UUID.fromString("53f53841-0bf3-467f-98e2-578d360ee573"))

  var cargoAggregateRootActor: Option[ActorRef] = None

  val userId1 = UserId(UUID.fromString("53f53841-0bf3-467f-98e2-578d360ee572"))

  val userContext = Some(new UserContext {
    override def roles: List[String] = List.empty

    override def userId: UserId = userId1
  })

  val metaData = MetaData(ZonedDateTime.now(ZoneOffset.UTC), userContext, None)

  override def router(): ActorRef = {
    cargoAggregateRootActor = Some(cargoAggregateRootActor.getOrElse(system.actorOf(Props(classOf[Cargo], cargoId1))))
    cargoAggregateRootActor.get
  }

  implicit val timeout = Timeout(10.seconds) //dilated

  "CargoAggregateRoot" must {

    "Plan a new Cargo Delivery" in {
      val routeSpecification = RouteSpecification(Location("home"), Location("destination"), ZonedDateTime.parse("2018-03-03T10:15:30+01:00[Europe/Amsterdam]"))

      within(10.seconds) {
        router() ! PlanCargo(metaData, cargoId1, trackingId, routeSpecification)
        expectMsgPF() {
          case Right(msg: Seq[CargoDomainEvent]) =>
            (msg.filter(x => x.isInstanceOf[CargoPlanned]).size) should be(1)
          case other => fail(s"did not receive DriverEnrolled but $other")
        }
      }
    }

    "Send a wrong command to a smartdriver" in {
      within(10.seconds) {
        router() ! "WrongCommandAsThisIsAString"
        expectMsgPF() {
          case Left(ex) if ex.isInstanceOf[CommandNotProcessedException] => // expected
          case other => fail(s"did not receive ProcessingRecordRemoved but $other")
        }
      }
    }

    "Start session for this driver" in {
      within(10.seconds) {
        val routeSpecification = RouteSpecification(Location("home"), Location("destination"), ZonedDateTime.parse("2018-03-03T10:15:30+01:00[Europe/Amsterdam]"))

        router() ! SpecifyNewRoute(metaData, cargoId1, routeSpecification)
        expectMsgPF() {
          case Right(msg: Seq[NewRouteSpecified]) if msg.head.isInstanceOf[NewRouteSpecified] => // expected
          case other => fail(s"did not receive SessionStarted but $other")
        }
      }
    }
  }

}
