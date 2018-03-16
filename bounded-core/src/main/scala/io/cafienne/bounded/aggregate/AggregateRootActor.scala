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
import scala.reflect.ClassTag

trait AggregateRootCreator {
  def create[A <: AggregateRootActor :ClassTag](id: AggregateRootId): A

}

trait AggregateStateCreator {
  def newState(evt: DomainEvent): AggregateState
}

/**
  * The AggregateRootActor ensures focus on the transformation of domain commands towards domain event.
  */
trait AggregateRootActor extends PersistentActor with AggregateStateCreator with ActorLogging {

  /**
    * When extending the AggregateRootActor you must return the unique id of the aggregate root.
    * @return AggregateRootId
    */
  def aggregateId: AggregateRootId

  /**
    * When extending the AggregateRootActor you MUST implement the handleCommand method.
    *
    * @param command one of the commands your aggregate root expects
    * @param state of the Aggregate Root at the moment of handling command
    * @return a sequence of events when everything is Right, or an Failure (Left)
    */
  def handleCommand(command: DomainCommand, state: AggregateState): Reply


  // Below this line is the internal implementation of the Aggregate Root Actor.
  private var internalState: Option[AggregateState] = None

  def state: Option[AggregateState] = { internalState }

  private def updateState(evt: DomainEvent) {
    internalState = Some(internalState.fold(newState(evt))(_ update evt))
  }

  override def persistenceId: String = aggregateId.idAsString

  final def receiveCommand: Actor.Receive = {
    case cmd: DomainCommand =>
      val originalSender = sender()
      val reply = handleCommand(cmd, internalState getOrElse NoState)

      reply match {
        case Ok(events) => persistAll(events)(updateState)
        case Ko(_) => ()
      }

      originalSender ! reply

    case _: GetState.type =>
      sender() ! state.getOrElse(NoState)
  }

  override def receiveRecover: Receive = {
    case _: RecoveryCompleted => // initialize further processing when required
    case evt: DomainEvent => updateState(evt)
    case other => log.warning("Received unknown event {} during recovery", other)
  }

}

object AggregateRootActor {
  case object GetState
  case object NoState extends AggregateState {
    override def update(evt: DomainEvent): AggregateState = NoState
  }

}
