/*
 * Copyright (C) 2016-2023 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.aggregate

import akka.actor.{ActorSystem, Props}
import scala.collection.immutable.Seq
import ClassicSimpleAggregate._

class ClassicSimpleAggregate(aggregateRootId: String) extends AggregateRootActor[ClassicSimpleAggregateState] {

  override def aggregateId: String = aggregateRootId

  override def handleCommand(command: DomainCommand, aggregateState: Option[ClassicSimpleAggregateState]): Reply = {
    command match {
      case Stop(aggregateRootId) =>
        context.stop(self)
        Ok(Seq.empty)
      case Create(aggregateRootId) =>
        Ok(List(Created(aggregateRootId)))
      case AddItem(aggregateRootId, newItem) =>
        Ok(
          Seq(
            ItemAdded(
              aggregateRootId,
              newItem
            )
          )
        )
      case GetItems(aggregateRootId) =>
        aggregateState.fold(Ko(Items(Seq.empty)))(state => Ko(Items(state.items.toSeq)))
      case other => Ko(new UnexpectedCommand(other))
    }
  }

  override def newState(evt: DomainEvent): Option[ClassicSimpleAggregateState] = {
    evt match {
      case Created(aggregateRootId) =>
        log.debug("Create new state based on event {}", evt)
        Some(ClassicSimpleAggregateState(List.empty))
      case _ =>
        log.error("Event {} is not valid to create a new TestAggregateRootState")
        throw new IllegalArgumentException(s"Event $evt is not valid to create a new TestAggregateRootState")
    }
  }
}

object ClassicSimpleAggregate {

  trait SimpleCommand                              extends DomainCommand
  final case class Create(aggregateRootId: String) extends SimpleCommand
  final case class Stop(aggregateRootId: String)   extends SimpleCommand
  final case class StopAfter(
    aggregateRootId: String,
    waitInSecs: Integer
  ) extends SimpleCommand
  final case class AddItem(
    aggregateRootId: String,
    item: String
  ) extends SimpleCommand
  final case class GetItems(aggregateRootId: String) extends SimpleCommand

  //For testing there is no CQRS pattern and this misuses the HandlingFailure response to
  //get part of the state back for verification during a test
  case class Items(items: Seq[String]) extends HandlingFailure

  trait SimpleEvent                                    extends DomainEvent
  final case class Created(id: String)                 extends SimpleEvent
  final case class ItemAdded(id: String, item: String) extends SimpleEvent

  case class ClassicSimpleAggregateState(items: List[String]) extends AggregateState[ClassicSimpleAggregateState] {
    override def update(event: DomainEvent): Option[ClassicSimpleAggregateState] = {
      event match {
        case evt: Created =>
          Some(this)
        case evt: ItemAdded =>
          Some(this.copy(items = items :+ evt.item))
        case other => throw new IllegalArgumentException(s"Cannot update state based on event $other")
      }
    }
  }

  val aggregateRootTag = "ar-simpleclassic"
}

class ClassicSimpleAggregateCreator(system: ActorSystem) extends AggregateRootCreator {

  override def props(aggregateRootId: String): Props = {
    system.log.debug("Returning new Props for {}", aggregateRootId)
    Props(
      classOf[ClassicSimpleAggregate],
      aggregateRootId
    )
  }

}
