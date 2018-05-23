/*
 * Copyright (C) 2016-2018 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.aggregate

import java.time.ZonedDateTime

import scala.collection.immutable.Seq
import stamina.Persistable

trait Id {
  def idAsString: String
}

trait AggregateRootId extends Id

trait UserId extends Id

trait UserContext {
  def userId: UserId

  def roles: List[String]
}

case class MetaData(timestamp: ZonedDateTime, userContext: Option[UserContext])

trait DomainCommand {
  def id: AggregateRootId

  def metaData: MetaData
}

trait DomainEvent extends Persistable {

  def id: AggregateRootId

  def metaData: MetaData
}

trait HandlingFailure

class AggregateNotInitialized(id: AggregateRootId) extends HandlingFailure
class UnexpectedCommand(command: DomainCommand)    extends HandlingFailure

sealed trait Reply

object Reply {
  def ok(events: Seq[DomainEvent]): Reply = Ok(events)
  def ko(failure: HandlingFailure): Reply = Ko(failure)
}

case class Ok(events: Seq[DomainEvent]) extends Reply

case class Ko(failure: HandlingFailure) extends Reply
