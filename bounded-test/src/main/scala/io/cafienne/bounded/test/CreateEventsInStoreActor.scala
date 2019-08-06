/*
 * Copyright (C) 2016-2019 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.test

import akka.actor.{ActorContext, ActorRef}
import akka.persistence.PersistentActor
import io.cafienne.bounded.aggregate.{DomainEvent, AggregateRootId}

class CreateEventsInStoreActor(aggregateId: AggregateRootId) extends PersistentActor {
  override def persistenceId: String = aggregateId.idAsString

  override def receiveRecover: Receive = {
    case other =>
      context.system.log.debug("received unknown event to recover:" + other)
  }

  override def receiveCommand: Receive = {
    case evt: DomainEvent => storeAndReply(sender(), evt)
    case other            => context.system.log.error(s"cannot handle command $other")
  }

  private def storeAndReply(replyTo: ActorRef, evt: Any)(implicit context: ActorContext): Unit = {
    persist(evt) { e =>
      context.system.log.debug(s"persisted $e")
      replyTo ! e
    }
  }

}
