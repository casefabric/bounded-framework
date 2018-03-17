/*
 * Copyright (C) 2016-2018 Cafienne B.V. <https://www.cafienne.io/bounded>
 */
package io.cafienne.bounded.cargosample.projections

import java.time.ZonedDateTime
import java.util.UUID
import akka.actor.{ActorSystem, Props}
import akka.event.{Logging, LoggingAdapter}
import akka.testkit.{TestKit, _}
import akka.util.Timeout
import io.cafienne.bounded.akka.persistence.eventmaterializers.{EventMaterializers, OffsetTypeSequence, ReadJournalOffsetStore}
import io.cafienne.bounded.test.CreateEventsInStoreActor
import io.cafienne.bounded.aggregate.{MetaData, UserContext, UserId}
import io.cafienne.bounded.cargosample.SpecConfig
import io.cafienne.bounded.cargosample.domain.CargoDomainProtocol._
import io.cafienne.bounded.cargosample.projections.QueriesJsonProtocol.CargoViewItem
import org.scalatest._

import scala.concurrent.duration._
import scala.concurrent.Await

class CargoQueriesSpec extends TestKit(ActorSystem("testsystem", SpecConfig.testConfigDVriendInMem))
    with WordSpecLike with Matchers with BeforeAndAfterAll {

  implicit val timeout = Timeout(10.seconds)
  implicit val logger: LoggingAdapter = Logging(system, getClass)

  val expectedDeliveryTime = ZonedDateTime.parse("2018-01-01T17:43:00+01:00")
  val userId1 = UserId(UUID.fromString("53f53841-0bf3-467f-98e2-578d360ee572"))
  val userId2 = UserId(UUID.fromString("42f53841-0bf3-467f-98e2-578d360ed46f"))
  private val userContext = Some(new UserContext {

    override def roles: List[String] = List.empty

    override def userId: UserId = userId1
  })

  val metaData = MetaData(expectedDeliveryTime, None, None)
  val metaData2 = MetaData(expectedDeliveryTime, userContext, None)

  val cargoId1 = CargoId(UUID.fromString("93ea7372-3181-11e7-93ae-92361f002671"))
  val cargoId2 = CargoId(UUID.fromString("93ea7372-3181-11e7-93ae-92361f002672"))
  val trackingId = TrackingId(UUID.fromString("67C1FBD1-E634-4B4E-BF31-BBA6D039C264"))
  val routeSpecification = RouteSpecification(Location("Amsterdam"), Location("New York"), expectedDeliveryTime)

  //setup a journal with events for the writer
  val storeEventsActor = system.actorOf(Props(classOf[CreateEventsInStoreActor], cargoId1), "storeevents-actor")
  val tp = TestProbe()

  //boot the reader and check the written records
  object cargoQueries extends CargoQueriesImpl()

  val cargoWriter = new CargoViewProjectionWriter(system) with ReadJournalOffsetStore with OffsetTypeSequence
  val eventMaterializers = new EventMaterializers(List(cargoWriter))

  //Note that the startup replays the events and needs an extended timeout for that !
  Await.result(eventMaterializers.startUp(true), 10.seconds)

  "Cargo Query" must {
    "add and retrieve valid cargo" in {
      val evt1 = CargoPlanned(metaData, cargoId1, trackingId, routeSpecification)
      within(10.seconds) {
        tp.send(storeEventsActor, evt1)
        tp.expectMsg(evt1)
      }

      //TODO given way of testing needs a way to understand *when* events are processed in the view.
      //For now that is not possible yet THUS a sleep to ensure processing of the events.
      //IDEA is to create a TestableEventMaterializer that will solve these issues and create an easy way of testing.
      Thread.sleep(1000)

      within(10.seconds) {
        val cargo = Await.result(cargoQueries.getCargo(cargoId1), 5.seconds)
        cargo should be(CargoViewItem(cargoId1, "Amsterdam", "New York", expectedDeliveryTime))
      }
    }

  }

  override def beforeAll {
  }

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }
}

