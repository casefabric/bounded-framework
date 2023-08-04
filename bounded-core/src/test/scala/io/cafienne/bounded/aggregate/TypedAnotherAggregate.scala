/*
 * Copyright (C) 2016-2023 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.aggregate

import akka.actor.typed.Behavior
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.{Effect, EventSourcedBehavior, ReplyEffect}
import com.typesafe.scalalogging.Logger
import io.cafienne.bounded.aggregate.typed.TypedAggregateRootManager
import scala.concurrent.duration._
import akka.actor.typed.scaladsl.{Behaviors, TimerScheduler}
import akka.persistence.RecoveryCompleted

//This another aggregate is for testing the use of command gateways with multiple aggregates.
object TypedAnotherAggregate {
  val aggregateRootTag = "ar-another"
  import TypedSimpleAggregate._

  var replayed = false

  final val logger = Logger(TypedSimpleAggregate.getClass.getSimpleName)

  // Command handler logic
  def commandHandler(
    timers: TimerScheduler[SimpleAggregateCommand]
  ): (SimpleAggregateState, SimpleAggregateCommand) => ReplyEffect[SimpleAggregateEvent, SimpleAggregateState] = {
    (state, command) =>
      logger.debug("Received command: " + command)
      command match {
        case cmd: Create  => createAggregate(cmd)
        case cmd: AddItem => addItem(cmd)
        case cmd: Stop =>
          logger.debug("Stopping Aggregate {}", cmd.aggregateRootId)
          Effect.stop().thenReply(cmd.replyTo)(_ => OK)
        case cmd: StopAfter =>
          timers.startSingleTimer(
            InternalStop(aggregateRootId = cmd.aggregateRootId, metaData = cmd.metaData),
            2.seconds
          )
          Effect.none.thenReply(cmd.replyTo)(_ => OK)
        case cmd: InternalStop =>
          logger.debug("InternalStop for aggregate {}", cmd.aggregateRootId)
          Effect.stop().thenNoReply()
      }
  }

  private def createAggregate(cmd: Create): ReplyEffect[SimpleAggregateEvent, SimpleAggregateState] = {
    logger.debug(s"Create Aggregate (replayed: $replayed)" + cmd)
    Effect.persist(Created(cmd.aggregateRootId, TestMetaData.fromCommand(cmd.metaData))).thenReply(cmd.replyTo)(_ ⇒ OK)
  }

  private def addItem(cmd: AddItem): ReplyEffect[SimpleAggregateEvent, SimpleAggregateState] = {
    logger.debug("AddItem " + cmd)
    Effect
      .persist(ItemAdded(cmd.aggregateRootId, TestMetaData.fromCommand(cmd.metaData), cmd.item))
      .thenReply(cmd.replyTo)(_ ⇒ OK)
  }

  // event handler to keep internal aggregate state
  val eventHandler: (SimpleAggregateState, SimpleAggregateEvent) => SimpleAggregateState = { (state, event) =>
    logger.debug("updating state with {}", event)
    state.update(event)
  }

}

import TypedSimpleAggregate._
class AnotherAggregateManager() extends TypedAggregateRootManager[SimpleAggregateCommand] {
  import TypedSimpleAggregate._
  final val logger = Logger(this.getClass.getSimpleName)

  override def behavior(id: String): Behavior[SimpleAggregateCommand] = {
    logger.debug("Create aggregate behavior for {}", id)
    Behaviors.withTimers { timers ⇒
      EventSourcedBehavior
        .withEnforcedReplies(
          PersistenceId(aggregateRootTag, id),
          SimpleAggregateState(List.empty[String]),
          commandHandler(timers),
          eventHandler
        )
        .receiveSignal {
          case (state, b: RecoveryCompleted) => {
            logger.debug("Received RecoveryCompleted for actor with state {}", state)
            replayed = true
          }
        }
    }
  }

  override def entityTypeKey: EntityTypeKey[SimpleAggregateCommand] =
    EntityTypeKey[SimpleAggregateCommand](aggregateRootTag)
}
