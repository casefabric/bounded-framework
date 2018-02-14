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
package io.cafienne.bounded.test

import akka.actor.{ActorContext, ActorRef}
import akka.persistence.PersistentActor
import io.cafienne.bounded.commands.AggregateRootEvent

class CreateEventsInStoreActor extends PersistentActor {
  override def persistenceId: String = "bounded.test-events-actor"

  override def receiveRecover: Receive = {
    case other =>
      context.system.log.debug("received unknown event to recover:" + other)
  }

  override def receiveCommand: Receive = {
    case evt: AggregateRootEvent => storeAndReply(sender(), evt)
    case other                   => context.system.log.error(s"cannot handle command $other")
  }

  private def storeAndReply(replyTo: ActorRef, evt: Any)(
      implicit context: ActorContext): Unit = {
    persist(evt) { e =>
      context.system.log.debug(s"persisted $e")
      replyTo ! e
    }
  }

}
