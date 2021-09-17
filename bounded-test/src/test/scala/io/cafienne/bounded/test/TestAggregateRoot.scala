/*
 * Copyright (C) 2016-2021 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.test

import akka.actor.{ActorSystem, Props}
import io.cafienne.bounded.aggregate._
import io.cafienne.bounded.test.DomainProtocol.StateUpdated
import io.cafienne.bounded.test.TestAggregateRoot.TestAggregateRootState
import scala.collection.immutable.Seq

class TestAggregateRoot(aggregateRootId: String) extends AggregateRootActor[TestAggregateRootState] {
  import DomainProtocol._

  override def aggregateId: String = aggregateRootId

  override def handleCommand(command: DomainCommand, aggregateState: Option[TestAggregateRootState]): Reply = {
    command match {
      case CreateInitialState(metaData, aggregateRootId, state) =>
        val testMetaData = metaData.asInstanceOf[TestCommandMetaData]
        Ok(
          Seq[DomainEvent](
            InitialStateCreated(
              TestMetaData.fromCommand(testMetaData),
              aggregateRootId,
              state
            )
          )
        )
      case UpdateState(metaData, aggregateRootId, state) =>
        val testMetaData = metaData.asInstanceOf[TestCommandMetaData]
        if (aggregateState.isDefined && aggregateState.get.state.equals("new")) {
          Ok(
            Seq(
              StateUpdated(
                TestMetaData.fromCommand(testMetaData),
                aggregateRootId,
                state
              )
            )
          )
        } else {
          Ko(StateTransitionForbidden(aggregateState.map(_.state), state))
        }
      case UpdateStateSlow(metaData, aggregateRootId, state, waitFor) =>
        Thread.sleep(waitFor.length)
        val testMetaData = metaData.asInstanceOf[TestCommandMetaData]
        if (aggregateState.isDefined) {
          Ok(
            Seq(
              SlowStateUpdated(
                TestMetaData.fromCommand(testMetaData),
                aggregateRootId,
                state,
                waitFor.length
              )
            )
          )
        } else {
          Ko(StateTransitionForbidden(aggregateState.map(_.state), state))
        }
      case other => Ko(new UnexpectedCommand(other))
    }
  }

  override def newState(evt: DomainEvent): Option[TestAggregateRootState] = {
    evt match {
      case InitialStateCreated(metaData, id, state) =>
        log.debug("Create new state based on event {}", evt)
        Some(TestAggregateRootState(state))
      case _ =>
        log.error("Event {} is not valid to create a new TestAggregateRootState")
        throw new IllegalArgumentException(s"Event $evt is not valid to create a new TestAggregateRootState")
    }
  }
}

object TestAggregateRoot {

  case class TestAggregateRootState(state: String) extends AggregateState[TestAggregateRootState] {
    override def update(event: DomainEvent): Option[TestAggregateRootState] = {
      event match {
        case evt: StateUpdated =>
          Some(this.copy(state = evt.state))
        case other => throw new IllegalArgumentException(s"Cannot update state based on event $other")
      }
    }
  }

  val aggregateRootTag = "ar-test"
}

class TestAggregateRootCreator(system: ActorSystem) extends AggregateRootCreator {

  override def props(aggregateRootId: String): Props = {
    system.log.debug("Returning new Props for {}", aggregateRootId)
    Props(
      classOf[TestAggregateRoot],
      aggregateRootId
    )
  }

}
