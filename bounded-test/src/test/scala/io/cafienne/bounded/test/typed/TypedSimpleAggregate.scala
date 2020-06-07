/*
 * Copyright (C) 2016-2020 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.test.typed

import java.time.{OffsetDateTime, ZonedDateTime}
import java.util.UUID

import akka.actor.typed.scaladsl.{Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior}
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import akka.persistence.RecoveryCompleted
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.{Effect, EventSourcedBehavior, ReplyEffect}
import com.typesafe.scalalogging.Logger
import io.cafienne.bounded.aggregate.{CommandMetaData, DomainCommand, DomainEvent, MetaData}
import io.cafienne.bounded.aggregate.typed.TypedAggregateRootManager
import io.cafienne.bounded.{BuildInfo, RuntimeInfo, UserContext}

import scala.concurrent.duration._

object TypedSimpleAggregate {
  val aggregateRootTag = "ar-simple"

  implicit val buildInfo   = BuildInfo("spec", "1.0")
  implicit val runtimeInfo = RuntimeInfo("current")

  var replayed = false

  final val logger = Logger(TypedSimpleAggregate.getClass.getSimpleName)
  //Reply Type
  sealed trait Response
  case object OK                              extends Response
  final case class KO(reason: String)         extends Response
  final case class Items(items: List[String]) extends Response

  // Command Type
  final case class AggregateCommandMetaData(timestamp: OffsetDateTime, userContext: Option[UserContext])
      extends CommandMetaData

  sealed trait SimpleAggregateCommand extends DomainCommand

  final case class Create(aggregateRootId: String, metaData: CommandMetaData, replyTo: ActorRef[Response])
      extends SimpleAggregateCommand

  final case class AddItem(
    aggregateRootId: String,
    metaData: CommandMetaData,
    item: String,
    replyTo: ActorRef[Response]
  ) extends SimpleAggregateCommand

  final case class Stop(aggregateRootId: String, metaData: CommandMetaData, replyTo: ActorRef[Response])
      extends SimpleAggregateCommand

  private final case class InternalStop(aggregateRootId: String, metaData: CommandMetaData)
      extends SimpleAggregateCommand

  final case class StopAfter(
    aggregateRootId: String,
    metaData: CommandMetaData,
    waitInSecs: Integer,
    replyTo: ActorRef[Response]
  ) extends SimpleAggregateCommand

  final case class TriggerError(aggregateRootId: String, metaData: CommandMetaData, replyTo: ActorRef[Response])
      extends SimpleAggregateCommand

  //NOTE that a GET on the aggregate is not according to the CQRS pattern and added here for testing.
  sealed trait SimpleDirectAggregateQuery extends SimpleAggregateCommand

  final case class GetItems(aggregateRootId: String, metaData: CommandMetaData, replyTo: ActorRef[Response])
      extends SimpleDirectAggregateQuery

  // Event Type
  sealed trait SimpleAggregateEvent                                                  extends DomainEvent
  final case class Created(id: String, metaData: MetaData)                           extends SimpleAggregateEvent
  final case class ItemAdded(id: String, metaData: MetaData, item: String)           extends SimpleAggregateEvent
  final case class StoppedAfter(id: String, metaData: MetaData, waitInSecs: Integer) extends SimpleAggregateEvent

  // State Type
  final case class SimpleAggregateState(items: List[String] = List.empty[String], aggregateId: String) {
    def update(evt: SimpleAggregateEvent): SimpleAggregateState = {
      evt match {
        case evt: Created      => this.copy(aggregateId = evt.id)
        case evt: ItemAdded    => this.copy(items = items.::(evt.item))
        case evt: StoppedAfter => this
      }
    }
  }

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
          //timers.cancelAll()
          Effect.stop().thenReply(cmd.replyTo)(_ => OK)
        //case cmd: GetItems => getItems(cmd)
        case cmd: StopAfter =>
          timers.startSingleTimer(
            InternalStop(aggregateRootId = cmd.aggregateRootId, metaData = cmd.metaData),
            2.seconds
          )
          Effect.none.thenReply(cmd.replyTo)(_ => OK)
        case cmd: InternalStop =>
          logger.debug("InternalStop for aggregate {}", cmd.aggregateRootId)
          Effect.stop().thenNoReply()
        case cmd: TriggerError =>
          throw new IllegalArgumentException(
            "This is an exception thrown during processing the command for AR " + state.aggregateId
          )
          Effect.none.thenReply(cmd.replyTo)(_ => OK)
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

  private def stopAfter(cmd: StopAfter): ReplyEffect[SimpleAggregateEvent, SimpleAggregateState] = {
    logger.debug("StoppedAfter " + cmd)
    Effect
      .persist(StoppedAfter(cmd.aggregateRootId, TestMetaData.fromCommand(cmd.metaData), cmd.waitInSecs))
      .thenReply(cmd.replyTo)(_ ⇒ OK)
  }

  //  private def getItems(cmd: GetItems): ReplyEffect[SimpleAggregateEvent, SimpleAggregateState] = {
  //    Effect.reply(cmd)(_ => OK)
  //  }

  // event handler to keep internal aggregate state
  val eventHandler: (SimpleAggregateState, SimpleAggregateEvent) => SimpleAggregateState = { (state, event) =>
    logger.debug("updating state with {}", event)
    state.update(event)
  }

}

import io.cafienne.bounded.test.typed.TypedSimpleAggregate._

class SimpleAggregateManager() extends TypedAggregateRootManager[SimpleAggregateCommand] {
  import TypedSimpleAggregate._
  final val logger = Logger(this.getClass.getSimpleName)

  override def behavior(id: String): Behavior[SimpleAggregateCommand] = {
    logger.debug("Create aggregate behavior for {}", id)
    Behaviors.withTimers { timers ⇒
      EventSourcedBehavior
        .withEnforcedReplies(
          PersistenceId(aggregateRootTag, id),
          SimpleAggregateState(List.empty[String], id),
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

case class TestMetaData(
  timestamp: OffsetDateTime,
  userContext: Option[UserContext],
  causedByCommand: Option[UUID],
  buildInfo: BuildInfo,
  runTimeInfo: RuntimeInfo
) extends MetaData

object TestMetaData {
  def fromCommand(
    metadata: CommandMetaData
  )(implicit buildInfo: BuildInfo, runtimeInfo: RuntimeInfo): TestMetaData = {
    TestMetaData(
      metadata.timestamp,
      metadata.userContext,
      Some(metadata.commandId),
      buildInfo,
      runtimeInfo
    )
  }
}
