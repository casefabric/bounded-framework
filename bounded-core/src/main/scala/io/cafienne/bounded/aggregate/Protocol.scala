/*
 * Copyright (C) 2016-2018 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.aggregate

import java.time.ZonedDateTime
import java.util.UUID

import io.cafienne.bounded.{BuildInfo, Id, RuntimeInfo, UserContext}

import scala.collection.immutable.Seq
import stamina.Persistable

trait AggregateRootId extends Id

/**
  * Metadata of the event contains data that is used within the framework and may be used by the application
  * @param timestamp the moment the event was created
  * @param userContext contains an assumed known user, events generated via an HTTP API will most of the time be authenticated
  */
trait CommandMetaData {
  def timestamp: ZonedDateTime
  def userContext: Option[UserContext]
  def commandId: UUID = UUID.randomUUID()
}

trait DomainCommand {

  def aggregateRootId: AggregateRootId

  def metaData: CommandMetaData
}

/**
  * Metadata of the event contains data that is used within the framework and may be used by the application
  * @param timestamp the moment the event was created
  * @param userContext contains an assumed known user, events generated via an HTTP API will most of the time be authenticated
  * @param causedByCommand contains a reference to the command that has caused this event.
  * @param buildInfo contains the build information of the application. The Version is used to ensure version specific routing of messages.
  * @param runTimeInfo contains information on the runtime the event is generated and stored
  */
trait MetaData {
  def timestamp: ZonedDateTime
  def userContext: Option[UserContext]
  def causedByCommand: Option[UUID]
  def buildInfo: BuildInfo
  def runTimeInfo: RuntimeInfo
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
