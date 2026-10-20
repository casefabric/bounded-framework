/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.aggregate

import org.apache.pekko.actor.typed.ActorRef
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

/**
  * The journal reported a failure while storing the events of a command; the aggregate stops.
  * The events may or may not have been stored: the next command recovers the aggregate and shows which.
  */
final case class PersistenceFailed(cause: Throwable) extends HandlingFailure

/** The journal rejected the events of a command (e.g. they could not be serialized); the aggregate keeps running. */
final case class PersistenceRejected(cause: Throwable) extends HandlingFailure

sealed trait Reply

object Reply {
  def ok(events: Seq[DomainEvent]): Reply = Ok(events)
  def ko(failure: HandlingFailure): Reply = Ko(failure)
}

case class Ok(events: Seq[DomainEvent]) extends Reply

case class Ko(failure: HandlingFailure) extends Reply
