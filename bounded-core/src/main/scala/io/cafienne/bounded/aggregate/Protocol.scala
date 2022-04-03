/*
 * Copyright (C) 2016-2022 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.aggregate

import akka.actor.typed.ActorRef
import scala.collection.immutable.Seq

trait DomainCommand {
  def aggregateRootId: String
}

trait ReplyTo {
  var replyTo: ActorRef[_]
}

trait DomainEvent {
  def id: String
}

trait HandlingFailure

//class AggregateNotInitialized(id: PersistenceId) extends HandlingFailure
class UnexpectedCommand(command: DomainCommand) extends HandlingFailure

sealed trait Reply

object Reply {
  def ok(events: Seq[DomainEvent]): Reply = Ok(events)
  def ko(failure: HandlingFailure): Reply = Ko(failure)
}

case class Ok(events: Seq[DomainEvent]) extends Reply

case class Ko(failure: HandlingFailure) extends Reply
