/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.aggregate.typed

import org.apache.pekko.persistence.typed.PersistenceId
import org.apache.pekko.persistence.typed.scaladsl.{Effect, EventSourcedBehavior, ReplyEffect}
import io.cafienne.bounded.aggregate.AggregateType

/**
  * Builds the `EventSourcedBehavior` of a typed aggregate: it persists under the persistence id the gateway gives
  * (`aggregateType|aggregateRootId`) and tags every event with the aggregate type, so event materializers find its
  * events on every journal. The result is a plain `EventSourcedBehavior`, so `.snapshotWhen`, `.withRetention`,
  * `.receiveSignal` etc. can be chained as usual.
  *
  * {{{
  * override def behavior(persistenceId: PersistenceId): Behavior[OrderCommand] =
  *   AggregateBehavior.withEnforcedReplies(persistenceId, OrderState.empty, commandHandler, eventHandler)
  * }}}
  */
object AggregateBehavior {

  def apply[Command, Event, State](
    persistenceId: PersistenceId,
    emptyState: State,
    commandHandler: (State, Command) => Effect[Event, State],
    eventHandler: (State, Event) => State
  ): EventSourcedBehavior[Command, Event, State] = {
    val tag = typeTag(persistenceId)
    EventSourcedBehavior[Command, Event, State](persistenceId, emptyState, commandHandler, eventHandler)
      .withTagger(_ => Set(tag))
  }

  def withEnforcedReplies[Command, Event, State](
    persistenceId: PersistenceId,
    emptyState: State,
    commandHandler: (State, Command) => ReplyEffect[Event, State],
    eventHandler: (State, Event) => State
  ): EventSourcedBehavior[Command, Event, State] = {
    val tag = typeTag(persistenceId)
    EventSourcedBehavior
      .withEnforcedReplies[Command, Event, State](persistenceId, emptyState, commandHandler, eventHandler)
      .withTagger(_ => Set(tag))
  }

  private def typeTag(persistenceId: PersistenceId): String =
    AggregateType
      .of(persistenceId.id)
      .getOrElse(
        throw new IllegalArgumentException(
          s"Persistence id [${persistenceId.id}] has no aggregate type; use the persistence id the gateway passes to " +
            "TypedAggregateRootManager.behavior (aggregateType|aggregateRootId)"
        )
      )
      .name
}
