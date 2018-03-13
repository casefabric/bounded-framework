// Copyright (C) 2018 the original author or authors.
// See the LICENSE file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package io.cafienne.bounded.aggregate

import akka.actor._
import akka.persistence.{PersistentActor, RecoveryCompleted}
import io.cafienne.bounded.aggregate.AggregateRootActor.{GetState, NoState}

import scala.collection.immutable.Seq
import scala.reflect.ClassTag

trait AggregateRootCreator {
  def create[A <: AggregateRootActor :ClassTag](id: AggregateRootId): A

}

trait AggregateRootStateCreator {
  def newState(evt: AggregateRootEvent): AggregateRootState
}

/**
  * The AggregateRootActor ensures focus on the transformation of domain commands towards domain event.
  */
trait AggregateRootActor extends PersistentActor with AggregateRootStateCreator with ActorLogging {

  /**
    * When extending the AggregateRootActor you must return the unique id of the aggregate root.
    * @return AggregateRootId
    */
  def aggregateId: AggregateRootId

  /**
    * When extending the AggregateRootActor you MUST implement the handleCommand method.
    *
    * @param command is the commands your aggregate root expects
    * @param currentState of the Aggregate Root
    * @return a seqency of events when everything is Right, or an Exception (Left)
    */                                                                                //TODO replace Either (no EXCEPTION)
  def handleCommand(command: AggregateRootCommand, currentState: AggregateRootState): Either[Exception, Seq[AggregateRootEvent]]

  // Below this line is the internal implementation of the Aggregate Root Actor.
  private var internalState: Option[AggregateRootState] = None

  def state: Option[AggregateRootState] = { internalState }

  private def updateState(evt: AggregateRootEvent) {
    internalState = Some(internalState.fold(newState(evt))(state => state.update(evt)))
  }

  override def persistenceId: String = aggregateId.idAsString

  final def receiveCommand: Actor.Receive = {
    case cmd: AggregateRootCommand =>
      val originalSender = sender()
      handleCommand(cmd, internalState.fold(NoState: AggregateRootState)(state => state)) match {
        case Right(evt) =>
          persistAll[AggregateRootEvent](evt) { e =>
            updateState(e)
          }
          originalSender ! Right(evt)
        case Left(exc) => originalSender ! Left(CommandNotProcessedException("Could not handle command.", exc))
      }

    case _: GetState.type =>
      state.fold(sender() ! NoState)(state => sender() ! state)
  }

  override def receiveRecover: Receive = {
    case _: RecoveryCompleted => // initialize further processing when required
    case evt: AggregateRootEvent => updateState(evt)
    case other => log.warning("Received unknown event {} during recovery", other)
  }

}

object AggregateRootActor {
  case object GetState
  case object NoState extends AggregateRootState {
    override def update(evt: AggregateRootEvent): AggregateRootState = NoState
  }

}
