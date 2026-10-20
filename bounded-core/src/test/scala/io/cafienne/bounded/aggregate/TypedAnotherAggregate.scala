/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.aggregate

import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.cluster.sharding.typed.scaladsl.EntityTypeKey
import org.apache.pekko.persistence.typed.PersistenceId
import org.apache.pekko.persistence.typed.scaladsl.{Effect, EventSourcedBehavior, ReplyEffect}
import com.typesafe.scalalogging.Logger
import io.cafienne.bounded.aggregate.typed.TypedAggregateRootManager
import org.apache.pekko.persistence.typed.RecoveryCompleted
import io.cafienne.bounded.SampleProtocol.{CommandMetaData, MetaData}
import io.cafienne.bounded.aggregate.TypedSimpleAggregate.{Items, OK, Response}

//This another aggregate is for testing the use of command gateways with multiple aggregates.
//It has its own (sealed) protocol, next to the one of the TypedSimpleAggregate; only the replies are shared.
object TypedAnotherAggregate {
  val aggregateRootTag = "ar-another"

  var replayed = false

  final val logger = Logger(TypedAnotherAggregate.getClass.getSimpleName)

  // Command Type
  sealed trait AnotherAggregateCommand extends DomainCommand

  final case class CreateAnother(aggregateRootId: String, metaData: CommandMetaData, replyTo: ActorRef[Response])
      extends AnotherAggregateCommand

  final case class AddAnother(
    aggregateRootId: String,
    metaData: CommandMetaData,
    item: String,
    replyTo: ActorRef[Response]
  ) extends AnotherAggregateCommand

  //NOTE that a GET on the aggregate is not according to the CQRS pattern and added here for testing.
  final case class GetAnotherItems(aggregateRootId: String, metaData: CommandMetaData, replyTo: ActorRef[Response])
      extends AnotherAggregateCommand

  final case class StopAnother(aggregateRootId: String, metaData: CommandMetaData, replyTo: ActorRef[Response])
      extends AnotherAggregateCommand

  // Event Type
  sealed trait AnotherAggregateEvent                                          extends DomainEvent
  final case class AnotherCreated(id: String, metaData: MetaData)             extends AnotherAggregateEvent
  final case class AnotherAdded(id: String, metaData: MetaData, item: String) extends AnotherAggregateEvent

  // State Type
  final case class AnotherAggregateState(items: List[String] = List.empty[String]) {
    def update(evt: AnotherAggregateEvent): AnotherAggregateState = {
      evt match {
        case _: AnotherCreated => this
        case evt: AnotherAdded => this.copy(items = items.::(evt.item))
      }
    }
  }

  // Command handler logic
  val commandHandler
    : (AnotherAggregateState, AnotherAggregateCommand) => ReplyEffect[AnotherAggregateEvent, AnotherAggregateState] = {
    (state, command) =>
      logger.debug("Received command: " + command)
      command match {
        case cmd: CreateAnother   => createAggregate(cmd)
        case cmd: AddAnother      => addAnother(cmd)
        case cmd: GetAnotherItems => Effect.none.thenReply(cmd.replyTo)(_ => Items(state.items))
        case cmd: StopAnother =>
          logger.debug("Stopping Aggregate {}", cmd.aggregateRootId)
          Effect.stop().thenReply(cmd.replyTo)(_ => OK)
      }
  }

  private def createAggregate(cmd: CreateAnother): ReplyEffect[AnotherAggregateEvent, AnotherAggregateState] = {
    logger.debug(s"Create Aggregate (replayed: $replayed)" + cmd)
    Effect
      .persist(AnotherCreated(cmd.aggregateRootId, TestMetaData.fromCommand(cmd.metaData)))
      .thenReply(cmd.replyTo)(_ => OK)
  }

  private def addAnother(cmd: AddAnother): ReplyEffect[AnotherAggregateEvent, AnotherAggregateState] = {
    logger.debug("AddAnother " + cmd)
    Effect
      .persist(AnotherAdded(cmd.aggregateRootId, TestMetaData.fromCommand(cmd.metaData), cmd.item))
      .thenReply(cmd.replyTo)(_ => OK)
  }

  // event handler to keep internal aggregate state
  val eventHandler: (AnotherAggregateState, AnotherAggregateEvent) => AnotherAggregateState = { (state, event) =>
    logger.debug("updating state with {}", event)
    state.update(event)
  }

}

import TypedAnotherAggregate._
class AnotherAggregateManager() extends TypedAggregateRootManager[AnotherAggregateCommand] {
  final val logger = Logger(this.getClass.getSimpleName)

  override def behavior(id: String): Behavior[AnotherAggregateCommand] = {
    logger.debug("Create Another aggregate behavior for {}", id)
    EventSourcedBehavior
      .withEnforcedReplies(
        PersistenceId(aggregateRootTag, id),
        AnotherAggregateState(),
        commandHandler,
        eventHandler
      )
      .receiveSignal {
        case (state, RecoveryCompleted) =>
          logger.debug("Received RecoveryCompleted for actor with state {}", state)
          replayed = true
      }
  }

  override def entityTypeKey: EntityTypeKey[AnotherAggregateCommand] =
    EntityTypeKey[AnotherAggregateCommand](aggregateRootTag)
}
