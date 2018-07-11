/*
 * Copyright (C) 2016-2018 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.aggregate

import java.time.ZonedDateTime
import java.util.UUID

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

/**
  * Metadata of the event contains data that is used within the framework and may be used by the application
  * @param timestamp the moment the event was created
  * @param userContext contains an assumed known user, events generated via an HTTP API will most of the time be authenticated
  */
case class CommandMetaData(timestamp: ZonedDateTime, userContext: Option[UserContext])


trait DomainCommand {

  def transactionId: UUID

  def aggregateRootId: AggregateRootId

  def metaData: CommandMetaData
}

/**
  * Version number of the build info is based on the Apache Runtime Versioning rules:
  * @see https://apr.apache.org/versioning.html
  * @param major
  * @param minor
  * @param patch
  */
case class Version(major: Integer, minor: Integer, patch: Integer)
//TODO it may not be a BuildInfo but a Runtime marker in order to confgure what components work together ?????
trait BuildInfo {
  def name: String
  def version: Version
}

/**
  * Metadata of the event contains data that is used within the framework and may be used by the application
  * @param timestamp the moment the event was created
  * @param userContext contains an assumed known user, events generated via an HTTP API will most of the time be authenticated
  * @param commandReference contains a reference to the command that has caused this event.
  * @param buildInfo contains the build information of the applicaiton. The Version is used to ensure version specific routing of messages.
  */
case class MetaData(timestamp: ZonedDateTime, userContext: Option[UserContext], commandReference: Option[String], buildInfo: BuildInfo)

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
