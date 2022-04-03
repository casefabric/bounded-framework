/*
 * Copyright (C) 2016-2022 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.test

import io.cafienne.bounded.aggregate.{DomainCommand, DomainEvent, HandlingFailure}

import java.time.OffsetDateTime
import java.util.UUID
import scala.concurrent.duration.Duration

object DomainProtocol {

  trait UserId {
    def idAsString: String
  }

  trait UserContext {
    def userId: UserId

    def roles: List[String]
  }

  trait CommandMetaData {
    def timestamp: OffsetDateTime

    def userContext: Option[UserContext]

    val commandId: UUID = UUID.randomUUID()
  }

  trait MetaData {
    def timestamp: OffsetDateTime

    def userContext: Option[UserContext]

    def causedByCommand: Option[UUID]
  }

  case class TestCommandMetaData(
    timestamp: OffsetDateTime,
    val userContext: Option[UserContext],
    override val commandId: UUID = UUID.randomUUID()
  ) extends CommandMetaData

  case class TestMetaData(
    timestamp: OffsetDateTime,
    userContext: Option[UserContext],
    causedByCommand: Option[UUID]
  ) extends MetaData

  object TestMetaData {
    def fromCommand(
      metadata: TestCommandMetaData
    ): TestMetaData = {
      TestMetaData(
        metadata.timestamp,
        metadata.userContext,
        Some(metadata.commandId)
      )
    }
  }

  case class CreateInitialState(metaData: CommandMetaData, aggregateRootId: String, state: String) extends DomainCommand

  case class InitialStateCreated(metaData: MetaData, id: String, state: String) extends DomainEvent

  case class UpdateState(metaData: CommandMetaData, aggregateRootId: String, state: String) extends DomainCommand

  case class UpdateStateSlow(metaData: CommandMetaData, aggregateRootId: String, state: String, waitFor: Duration)
      extends DomainCommand

  case class StateUpdated(metaData: MetaData, id: String, state: String) extends DomainEvent

  case class SlowStateUpdated(metaData: MetaData, id: String, state: String, waited: Long) extends DomainEvent

  //This can be sent but is not handled so gives a Ko(UnExpectedCommand)
  case class CommandWithoutHandler(metaData: CommandMetaData, aggregateRootId: String, msg: String)
      extends DomainCommand

  case class InvalidCommand(msg: String) extends HandlingFailure

  case class StateTransitionForbidden(from: Option[String], to: String) extends HandlingFailure

}
