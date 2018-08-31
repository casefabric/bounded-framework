/*
 * Copyright (C) 2016-2018 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.test

import akka.actor.{ActorSystem, Props}
import io.cafienne.bounded.{BuildInfo, RuntimeInfo}
import io.cafienne.bounded.aggregate._
import io.cafienne.bounded.test.DomainProtocol.StateUpdated
import io.cafienne.bounded.test.TestAggregateRoot.TestAggregateRootState

import scala.collection.immutable.Seq

object DomainProtocol {
  case class TestAggregateRootId(id: String) extends AggregateRootId {
    override def idAsString: String = id
  }

  case class CreateInitialState(metaData: CommandMetaData, aggregateRootId: AggregateRootId, state: String)
      extends DomainCommand
  case class InitialStateCreated(metaData: MetaData, id: AggregateRootId, state: String) extends DomainEvent

  case class UpdateState(metaData: CommandMetaData, aggregateRootId: AggregateRootId, state: String)
      extends DomainCommand
  case class StateUpdated(metaData: MetaData, id: AggregateRootId, state: String) extends DomainEvent

  case class InvalidCommand(msg: String) extends HandlingFailure
  case class InvalidState(msg: String)   extends HandlingFailure
}

class TestAggregateRoot(aggregateRootId: AggregateRootId, buildInfo: BuildInfo, runtimeInfo: RuntimeInfo)
    extends AggregateRootActor[TestAggregateRootState] {
  import DomainProtocol._

  implicit val bi = buildInfo
  implicit val ri = runtimeInfo

  override def aggregateId: AggregateRootId = aggregateRootId

  override def handleCommand(command: DomainCommand, aggregateState: Option[TestAggregateRootState]): Reply = {
    command match {
      case CreateInitialState(metaData, aggregateRootId, state) =>
        Ok(Seq[DomainEvent](InitialStateCreated(MetaData.fromCommand(metaData), aggregateRootId, state)))
      case UpdateState(metaData, aggregateRootId, state) =>
        if (aggregateState.isDefined && aggregateState.get.state.equals("new")) {
          Ok(Seq(StateUpdated(MetaData.fromCommand(metaData), aggregateRootId, state)))
        } else {
          Ko(InvalidState(s"The current state $aggregateState does not allow an update to $state"))
        }
      case other => Ko(new UnexpectedCommand(other))
    }
  }

  override def newState(evt: DomainEvent): Option[TestAggregateRootState] = {
    evt match {
      case InitialStateCreated(metaData, id, state) => Some(TestAggregateRootState(state))
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

class TestAggregateRootCreator(system: ActorSystem)(implicit buildInfo: BuildInfo, runtimeInfo: RuntimeInfo)
    extends AggregateRootCreator {

  override def props(aggregateRootId: AggregateRootId): Props = {
    system.log.debug("Returning new Props for {}", aggregateRootId)
    Props(classOf[TestAggregateRoot], aggregateRootId, buildInfo, runtimeInfo)
  }

}
