/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.aggregate.typed

import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors}
import org.apache.pekko.persistence.typed.PersistenceId
import org.apache.pekko.persistence.typed.scaladsl.{Effect, EventSourcedBehavior, ReplyEffect}
import io.cafienne.bounded.aggregate.{AggregateType, EventPosition}

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

  /**
    * `withEnforcedReplies` with the `ActorContext` in the command handler, for `positionOf`, `pipeToSelf` or logging.
    * The `EventSourcedBehavior` is built inside `Behaviors.setup`, so the result is a plain `Behavior`; change the
    * `EventSourcedBehavior` itself (`.snapshotWhen`, `.receiveSignal`, ...) with `adjust`.
    *
    * {{{
    * AggregateBehavior.withContext[OrderCommand, OrderEvent, OrderState](persistenceId, OrderState.empty,
    *   (context, state, command) => command match {
    *     case place: PlaceOrder =>
    *       val events = List(OrderPlaced(place.aggregateRootId))
    *       Effect.persist(events).thenReply(place.replyTo)(_ =>
    *         Persisted(events, AggregateBehavior.positionOf(context, persistenceId)))
    *   },
    *   (state, event) => state.update(event))
    * }}}
    */
  def withContext[Command, Event, State](
    persistenceId: PersistenceId,
    emptyState: State,
    commandHandler: (ActorContext[Command], State, Command) => ReplyEffect[Event, State],
    eventHandler: (State, Event) => State,
    adjust: EventSourcedBehavior[Command, Event, State] => EventSourcedBehavior[Command, Event, State] =
      (behavior: EventSourcedBehavior[Command, Event, State]) => behavior
  ): Behavior[Command] =
    Behaviors.setup { context =>
      adjust(
        withEnforcedReplies[Command, Event, State](
          persistenceId,
          emptyState,
          (state, command) => commandHandler(context, state, command),
          eventHandler
        )
      )
    }

  /**
    * The position of the last event the aggregate stored: its persistence id and sequence number. Use it in
    * `thenReply` after a persist, so the reply tells the caller what a read side must have processed to show the
    * command (see `readside.ReadSideRegistry`). It needs the `ActorContext`: build the behavior with `withContext`, or in
    * `Behaviors.setup`:
    *
    * {{{
    * override def behavior(persistenceId: PersistenceId): Behavior[OrderCommand] =
    *   Behaviors.setup { context =>
    *     AggregateBehavior.withEnforcedReplies[OrderCommand, OrderEvent, OrderState](
    *       persistenceId,
    *       OrderState.empty,
    *       (state, command) => command match {
    *         case place: PlaceOrder =>
    *           val events = List(OrderPlaced(place.aggregateRootId))
    *           Effect.persist(events).thenReply(place.replyTo)(_ =>
    *             Persisted(events, AggregateBehavior.positionOf(context, persistenceId)))
    *       },
    *       (state, event) => state.update(event)
    *     )
    *   }
    * }}}
    *
    * Only valid while the aggregate runs: in its command handler, event handler or the callbacks of its effects.
    */
  def positionOf(context: ActorContext[?], persistenceId: PersistenceId): EventPosition =
    EventPosition(persistenceId.id, EventSourcedBehavior.lastSequenceNumber(context))

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
