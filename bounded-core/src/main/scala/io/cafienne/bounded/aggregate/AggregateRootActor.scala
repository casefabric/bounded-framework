/*
 * Copyright (C) 2016-2023 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.aggregate

import akka.actor._
import akka.persistence.{PersistentActor, RecoveryCompleted, SnapshotOffer}
import io.cafienne.bounded.aggregate.AggregateRootActor.GetState

trait AggregateRootCreator {
  def props(idToCreate: String): Props
}

trait AggregateState[A <: AggregateState[A]] {
  def update(event: DomainEvent): Option[A]
}

trait AggregateStateCreator[A <: AggregateState[A]] {
  def newState(evt: DomainEvent): Option[A]
}

/**
  * The AggregateRootActor ensures focus on the transformation of domain commands towards domain event.
  */
trait AggregateRootActor[A <: AggregateState[A]]
    extends PersistentActor
    with AggregateStateCreator[A]
    with ActorLogging {

  /**
    * When extending the AggregateRootActor you must return the unique id of the aggregate root.
    * @return AggregateRootId
    */
  def aggregateId: String

  /**
    * When extending the AggregateRootActor you MUST implement the handleCommand method.
    *
    * @param command one of the commands your aggregate root expects
    * @param state of the Aggregate Root at the moment of handling command, optional: state could not be set yet
    * @return a sequence of events when everything is Right, or an Failure (Left)
    */
  def handleCommand(command: DomainCommand, state: Option[A]): Reply

  // Below this line is the internal implementation of the Aggregate Root Actor.
  private var internalState: Option[A] = None

  def state: Option[A] = { internalState }

  private def updateState(evt: DomainEvent): Unit = {
    internalState = internalState.fold(newState(evt))(_ update evt)
  }

  override def persistenceId: String = aggregateId

  val snapShotInterval: Option[Int] = None

  final def receiveCommand: Actor.Receive = {
    case cmd: DomainCommand =>
      val originalSender = sender()
      val reply          = handleCommand(cmd, internalState)

      reply match {
        case Ok(events) =>
          persistAll(events) { evt =>
            updateState(evt)
            if (snapShotInterval.isDefined) {
              if (lastSequenceNr % snapShotInterval.get == 0 && lastSequenceNr != 0) {
                saveSnapshot(state)
              }
            }
          }
        case Ko(_) => ()
      }

      originalSender ! reply

    case _: GetState.type =>
      sender() ! state
  }

  override def receiveRecover: Receive = {
    case _: RecoveryCompleted                  => // initialize further processing when required
    case SnapshotOffer(_, snapshot: Option[A]) => internalState = snapshot
    case evt: DomainEvent                      => updateState(evt)

    case other =>
      log.warning("Received unknown event {} during recovery", other)
  }
}

object AggregateRootActor {
  case object GetState
}
