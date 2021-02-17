/*
 * Copyright (C) 2016-2021 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.test

import java.time.OffsetDateTime
import akka.actor.ActorSystem
import akka.event.{Logging, LoggingAdapter}
import akka.testkit.TestKit
import akka.util.Timeout
import io.cafienne.bounded.test.TestableAggregateRoot._
import io.cafienne.bounded.test.DomainProtocol._
import io.cafienne.bounded.test.TestAggregateRoot.TestAggregateRootState
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpecLike

import scala.concurrent.duration._

class TestableAggregateRootSpec extends AsyncWordSpecLike with Matchers with ScalaFutures with BeforeAndAfterAll {

  //Setup required supporting classes
  implicit val timeout                = Timeout(10.seconds)
  implicit val system                 = ActorSystem("TestSystem", SpecConfig.testConfig)
  implicit val logger: LoggingAdapter = Logging(system, getClass)

  val testAggregateRootCreator = new TestAggregateRootCreator(system)

  val currentMeta = TestMetaData(OffsetDateTime.parse("2018-01-01T17:43:00+01:00"), None, None)
  val metaData    = TestCommandMetaData(currentMeta.timestamp, None)

  "The testable aggregate root" must {

    "create without state in given" in {
      val testAggregateRootId1 = "1"

      val ar = TestableAggregateRoot
        .given[TestAggregateRoot, TestAggregateRootState](testAggregateRootCreator, testAggregateRootId1)

      ar.currentState map { state => assert(state.isEmpty) }
    }

    "create initial state in given" in {
      val testAggregateRootId1 = "2"
      val targetState          = TestAggregateRootState("new")

      val ar = TestableAggregateRoot
        .given[TestAggregateRoot, TestAggregateRootState](
          testAggregateRootCreator,
          testAggregateRootId1,
          InitialStateCreated(
            currentMeta,
            testAggregateRootId1,
            "new"
          )
        )

      ar.currentState map { state =>
        assert(state.isDefined, s"There is no defined state but expected $targetState")
        assert(state.get == targetState)
      }
    }

    "handle the command to OK in when" in {
      val testAggregateRootId1 = "3"
      val commandMetaData      = TestCommandMetaData(currentMeta.timestamp, None)
      val updateStateCommand   = UpdateState(commandMetaData, testAggregateRootId1, "updated")
      val targetState          = TestAggregateRootState("updated")

      val ar = TestableAggregateRoot
        .given[TestAggregateRoot, TestAggregateRootState](
          testAggregateRootCreator,
          testAggregateRootId1,
          InitialStateCreated(
            currentMeta,
            testAggregateRootId1,
            "new"
          )
        )
        .when(updateStateCommand)

      ar.events should contain(
        StateUpdated(
          TestMetaData.fromCommand(commandMetaData),
          testAggregateRootId1,
          "updated"
        )
      )

      ar.currentState map { state =>
        assert(state.isDefined, s"There is no defined state but expected $targetState")
        assert(state.get == targetState)
      }
    }

    "provide command handling failure for assertions" in {
      val testAggregateRootId1 = "3"
      val updateStateCommand   = UpdateState(metaData, testAggregateRootId1, "updated")

      TestableAggregateRoot
        .given[TestAggregateRoot, TestAggregateRootState](
          testAggregateRootCreator,
          testAggregateRootId1,
          InitialStateCreated(
            currentMeta,
            testAggregateRootId1,
            "not_new"
          )
        )
        .when(updateStateCommand)
        .failure should be(StateTransitionForbidden(Some("not_new"), "updated"))
    }

    "prevent handling of commands that target another aggregate" in {
      val aggregateRootId = "4"
      val wrongId         = "5"

      an[MisdirectedCommand] should be thrownBy {
        TestableAggregateRoot
          .given[TestAggregateRoot, TestAggregateRootState](testAggregateRootCreator, aggregateRootId)
          .when(UpdateState(metaData, wrongId, ""))
      }
    }

    "signal that handling was successful despite expected failure" in {
      val aggregateRootId = "3"

      an[UnexpectedCommandHandlingSuccess] should be thrownBy {
        TestableAggregateRoot
          .given[TestAggregateRoot, TestAggregateRootState](
            testAggregateRootCreator,
            aggregateRootId,
            InitialStateCreated(currentMeta, aggregateRootId, "new")
          )
          .when(UpdateState(metaData, aggregateRootId, "updated"))
          .failure
      }
    }

    "tell that no point expecting failure when no commands were issued" in {
      val aggregateRootId = "3"

      an[NoCommandsIssued.type] should be thrownBy {
        TestableAggregateRoot
          .given[TestAggregateRoot, TestAggregateRootState](
            testAggregateRootCreator,
            aggregateRootId,
            InitialStateCreated(currentMeta, aggregateRootId, "new")
          )
          .failure
      }
    }

    "tell that no point expecting events when no commands were issued" in {
      val aggregateRootId = "3"

      an[NoCommandsIssued.type] should be thrownBy {
        TestableAggregateRoot
          .given[TestAggregateRoot, TestAggregateRootState](
            testAggregateRootCreator,
            aggregateRootId,
            InitialStateCreated(currentMeta, aggregateRootId, "new")
          )
          .events
      }
    }

    "give a clear failure when a command is processed that has no handler" in {
      val aggregateRootId = "4"

      an[CommandHandlingFailed] should be thrownBy {
        val ar = TestableAggregateRoot
          .given[TestAggregateRoot, TestAggregateRootState](
            testAggregateRootCreator,
            aggregateRootId,
            InitialStateCreated(currentMeta, aggregateRootId, "new")
          )
          .when(CommandWithoutHandler(metaData, aggregateRootId, "this one fails"))

        ar.events should be("throwing an CommandHandlingFailed")
      }

    }

  }

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system, 30.seconds, verifySystemShutdown = true)
  }

}
