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
import akka.persistence.PersistentActor
import io.cafienne.bounded.aggregate.AggregateRootActor.{GetState, NoState}

import scala.reflect.ClassTag

trait AggregateRootCreator {
  def create[A <: AggregateRootActor :ClassTag](id: AggregateRootId): A
}

trait AggregateRootActor extends PersistentActor {
  def state: Option[AggregateRootState]

  var commandReceivers: Actor.Receive = {
    case m: AggregateRootEvent =>
      persist(m) { e =>
        sender() ! e
      }
    case _: GetState.type =>
      state.fold(sender() ! NoState)(state => sender() ! state)
  }


  def commandHandler(next: Actor.Receive): Unit = { commandReceivers = commandReceivers orElse next }

  def receiveCommand: Actor.Receive = commandReceivers // Actor.receive definition

}

object AggregateRootActor {
  case object GetState
  case object NoState extends AggregateRootState

}
