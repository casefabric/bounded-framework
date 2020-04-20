/*
 * Copyright (C) 2016-2020 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.test

import akka.actor.{ActorContext, ActorRef}
import akka.persistence.PersistentActor
import akka.persistence.typed.PersistenceId
import io.cafienne.bounded.aggregate.DomainEvent

class CreateEventsInStoreActor(aggregateId: String) extends PersistentActor {
  override def persistenceId: String = aggregateId

  override def receiveRecover: Receive = {
    case other =>
      context.system.log.debug("received unknown event to recover:" + other)
  }

  override def receiveCommand: Receive = {
    case evt: DomainEvent => {
      persist(evt) { e =>
        context.system.log.debug(s"persisted $e")
        sender() ! e
      }
    }
    case other => context.system.log.error(s"cannot handle command $other")
  }

}
