/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.aggregate

import org.apache.pekko.actor._
import org.apache.pekko.persistence.{PersistentActor, RecoveryCompleted, SnapshotOffer}
import io.cafienne.bounded.aggregate.AggregateRootActor.{GetState, Passivate}

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

  // Sender of the command whose events are being persisted. persistAll stashes new commands until all
  // its handlers ran, so there is at most one; it is needed when the journal reports a failure.
  private var persistingFor: Option[ActorRef] = None

  final def receiveCommand: Actor.Receive = {
    case cmd: DomainCommand =>
      val originalSender = sender()
      val reply          = handleCommand(cmd, internalState)

      reply match {
        case Ok(events) if events.nonEmpty =>
          // Reply only when the last event is stored, so the caller never gets Ok for events that are not.
          val lastEventSequenceNr = lastSequenceNr + events.size
          persistingFor = Some(originalSender)
          persistAll(events) { evt =>
            updateState(evt)
            if (snapShotInterval.isDefined) {
              if (lastSequenceNr % snapShotInterval.get == 0 && lastSequenceNr != 0) {
                saveSnapshot(state)
              }
            }
            if (lastSequenceNr == lastEventSequenceNr) {
              persistingFor = None
              originalSender ! reply
            }
          }
        case _ =>
          originalSender ! reply
      }

    case _: GetState.type =>
      sender() ! state

    // Handled like a command, so after the commands before it, including those stashed during a persist.
    case Passivate =>
      context.stop(self)
  }

  /**
    * The journal reported a failure; the events may or may not have been stored. Pekko stops the actor after this call.
    */
  override protected def onPersistFailure(cause: Throwable, event: Any, seqNr: Long): Unit = {
    log.error(
      cause,
      "Failed to persist event {} with sequence number {} for persistenceId {}",
      event.getClass.getName,
      seqNr,
      persistenceId
    )
    replyPersistenceFailure(PersistenceFailed(cause))
  }

  /**
    * The journal rejected the events, nothing was stored. The actor keeps running.
    */
  override protected def onPersistRejected(cause: Throwable, event: Any, seqNr: Long): Unit = {
    log.warning(
      "Rejected to persist event {} with sequence number {} for persistenceId {}, reason: {}",
      event.getClass.getName,
      seqNr,
      persistenceId,
      cause.getMessage
    )
    replyPersistenceFailure(PersistenceRejected(cause))
  }

  // A batch of events fails or is rejected as a whole and the journal reports it per event: reply only once.
  private def replyPersistenceFailure(failure: HandlingFailure): Unit = {
    persistingFor.foreach(_ ! Ko(failure))
    persistingFor = None
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

  /**
    * Asks the aggregate to stop once it has handled the commands it received before.
    */
  case object Passivate
}
