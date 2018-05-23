/*
 * Copyright (C) 2016-2018 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.cargosample.projections

import java.io.File
import java.time.ZonedDateTime
import java.util.UUID

import akka.actor.ActorSystem
import akka.event.{Logging, LoggingAdapter}
import akka.persistence.query.Sequence
import akka.testkit.{TestKit, _}
import akka.util.Timeout
import io.cafienne.bounded.aggregate.{MetaData, UserContext, UserId}
import io.cafienne.bounded.akka.persistence.eventmaterializers.{OffsetStoreProvider, ReadJournalOffsetStore}
import io.cafienne.bounded.cargosample.SpecConfig
import io.cafienne.bounded.cargosample.domain.CargoDomainProtocol._
import io.cafienne.bounded.cargosample.projections.QueriesJsonProtocol.CargoViewItem
import io.cafienne.bounded.test.TestableProjection
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.time.{Millis, Seconds, Span}

import scala.concurrent.duration._

class CargoQueriesSpec extends WordSpec with Matchers with ScalaFutures with BeforeAndAfterAll {

  //Setup required supporting classes
  implicit val timeout                = Timeout(10.seconds)
  implicit val system                 = ActorSystem("CargoTestSystem", SpecConfig.testConfigDVriendInMem)
  implicit val logger: LoggingAdapter = Logging(system, getClass)
  implicit val defaultPatience        = PatienceConfig(timeout = Span(4, Seconds), interval = Span(100, Millis))

  //Create test data
  val expectedDeliveryTime = ZonedDateTime.parse("2018-01-01T17:43:00+01:00")
  val userId1              = CargoUserId(UUID.fromString("53f53841-0bf3-467f-98e2-578d360ee572"))
  val userId2              = CargoUserId(UUID.fromString("42f53841-0bf3-467f-98e2-578d360ed46f"))
  private val userContext = Some(new UserContext {

    override def roles: List[String] = List.empty

    override def userId: UserId = userId1
  })

  val metaData  = MetaData(expectedDeliveryTime, None)
  val metaData2 = MetaData(expectedDeliveryTime, userContext)

  val cargoId1           = CargoId(UUID.fromString("93ea7372-3181-11e7-93ae-92361f002671"))
  val cargoId2           = CargoId(UUID.fromString("93ea7372-3181-11e7-93ae-92361f002672"))
  val trackingId         = TrackingId(UUID.fromString("67C1FBD1-E634-4B4E-BF31-BBA6D039C264"))
  val routeSpecification = RouteSpecification(Location("Amsterdam"), Location("New York"), expectedDeliveryTime)

  val lmdbFile = new File("target", "cargo")
  val cargoLmdbClient = new CargoLmdbClient(lmdbFile)

  // Create Query and Writer for testing
  object cargoQueries extends CargoQueriesImpl(cargoLmdbClient)
  val cargoWriter = new CargoViewProjectionWriter(system, cargoLmdbClient) with OffsetStoreProvider

  "Cargo Query" must {
    "add and retrieve valid cargo based on replay" in {
      val evt1    = CargoPlanned(metaData, cargoId1, trackingId, routeSpecification)
      val fixture = TestableProjection.given(Seq(evt1))

      whenReady(fixture.startProjection(cargoWriter)) { replayResult =>
        logger.debug("replayResult: {}", replayResult)
        assert(replayResult.offset == Some(Sequence(1L)))
      }

      whenReady(cargoQueries.getCargo(cargoId1)) { cargo =>
        cargo should be(Some(CargoViewItem(cargoId1, "Amsterdam", "New York", expectedDeliveryTime)))
      }
    }
    "add and retrieve an update on valid cargo based on new event after replay" in {
      val evt1    = CargoPlanned(metaData, cargoId1, trackingId, routeSpecification)
      val fixture = TestableProjection.given(Seq(evt1))

      whenReady(fixture.startProjection(cargoWriter)) { replayResult =>
        logger.debug("replayResult: {}", replayResult)
        assert(replayResult.offset == Some(Sequence(1L)))
      }

      val evt2 = NewRouteSpecified(metaData, cargoId1, routeSpecification.copy(destination = Location("Oslo")))
      fixture.addEvent(evt2)
      whenReady(cargoQueries.getCargo(cargoId1)) { cargo =>
        cargo should be(Some(CargoViewItem(cargoId1, "Amsterdam", "Oslo", expectedDeliveryTime)))
      }

    }
  }

  override def afterAll {
    TestKit.shutdownActorSystem(system, 30.seconds, verifySystemShutdown = true)
    cleanupDb()
  }

  private def cleanupDb() = {
    val txn = cargoLmdbClient.env.txnWrite()
    try {
      cargoLmdbClient.dbi.drop(txn)
      txn.commit()
    } finally {
      txn.close()
    }
  }
}
