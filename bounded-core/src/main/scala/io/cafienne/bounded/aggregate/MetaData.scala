/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.aggregate

import java.time.{Clock, Instant}
import java.util.UUID

/**
  * Who sent a command, why, and when. Opt-in: a command that carries it extends `HasCommandMetaData`; `DomainCommand`
  * itself requires nothing.
  *
  * @param commandId     unique per command; the causation id of the events it causes
  * @param correlationId the same for every command and event that one request (or one import, one schedule) causes
  * @param causationId   the id of the event (or command) that caused this command, none for a request from outside
  * @param actor         who sent it: a user id, or a system actor such as `system:import`
  * @param timestamp     when the command was created
  * @param fields        application fields, such as the tenant
  */
final case class CommandMetaData(
  commandId: UUID,
  correlationId: String,
  causationId: Option[String],
  actor: String,
  timestamp: Instant,
  fields: Map[String, String]
)

object CommandMetaData {

  /** The metadata of a command from outside, e.g. a request: a new command id, no causation. */
  def create(
    correlationId: String,
    actor: String,
    fields: Map[String, String] = Map.empty,
    clock: Clock = Clock.systemUTC()
  ): CommandMetaData =
    CommandMetaData(UUID.randomUUID(), correlationId, None, actor, clock.instant(), fields)

  /**
    * The metadata of a command that an event causes, e.g. in a policy or combined logic: the correlation id and fields
    * of the event, the event as causation, and the actor of the event unless another is given.
    */
  def causedBy(event: EventMetaData, actor: Option[String] = None, clock: Clock = Clock.systemUTC()): CommandMetaData =
    CommandMetaData(
      UUID.randomUUID(),
      event.correlationId,
      Some(event.eventId.toString),
      actor.getOrElse(event.actor),
      clock.instant(),
      event.fields
    )

  /** The metadata of a command, when it carries any. */
  def of(command: Any): Option[CommandMetaData] = command match {
    case carrier: HasCommandMetaData => Option(carrier.metaData)
    case _                           => None
  }
}

/**
  * Who caused an event, why, and when. Opt-in: an event that carries it extends `HasEventMetaData`; `DomainEvent`
  * itself requires nothing. The aggregate fills it from its command with `EventMetaData.causedBy`.
  *
  * @param eventId       unique per event; the causation id of the commands it causes
  * @param correlationId the correlation id of the command
  * @param causationId   the command id of the command, or the id of whatever else caused the event
  * @param actor         the actor of the command
  * @param timestamp     when the event was created
  * @param fields        application fields, copied from the command
  * @param migrated      true for an event that was not caused by a command but imported from existing data
  */
final case class EventMetaData(
  eventId: UUID,
  correlationId: String,
  causationId: Option[String],
  actor: String,
  timestamp: Instant,
  fields: Map[String, String],
  migrated: Boolean
)

object EventMetaData {

  /** The metadata of an event that a command causes: its correlation id, actor and fields, the command as cause. */
  def causedBy(command: CommandMetaData, clock: Clock = Clock.systemUTC()): EventMetaData =
    EventMetaData(
      UUID.randomUUID(),
      command.correlationId,
      Some(command.commandId.toString),
      command.actor,
      clock.instant(),
      command.fields,
      migrated = false
    )

  /** The metadata of an event imported from existing data, e.g. the opening event of a migrated aggregate. */
  def imported(
    correlationId: String,
    actor: String,
    fields: Map[String, String] = Map.empty,
    clock: Clock = Clock.systemUTC()
  ): EventMetaData =
    EventMetaData(UUID.randomUUID(), correlationId, None, actor, clock.instant(), fields, migrated = true)

  /** The metadata of an event, when it carries any. */
  def of(event: Any): Option[EventMetaData] = event match {
    case carrier: HasEventMetaData => Option(carrier.metaData)
    case _                         => None
  }
}

/** A command that carries its `CommandMetaData`. */
trait HasCommandMetaData {
  def metaData: CommandMetaData
}

/** An event that carries its `EventMetaData`. The versioned JSON serializer stores it next to the event's own JSON. */
trait HasEventMetaData {
  def metaData: EventMetaData
}
