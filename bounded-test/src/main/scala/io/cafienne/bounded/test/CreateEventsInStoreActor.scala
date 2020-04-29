/*
 * Copyright (C) 2016-2020 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.test

import akka.actor.{ActorContext, ActorRef}
import akka.persistence.{PersistentActor, RecoveryCompleted}
import akka.persistence.journal.Tagged
import akka.persistence.typed.PersistenceId
import io.cafienne.bounded.aggregate.DomainEvent

class CreateEventsInStoreActor(aggregateId: String, tags: Set[String] = Set.empty) extends PersistentActor {
  override def persistenceId: String = aggregateId

  override def receiveRecover: Receive = {
    case RecoveryCompleted => // thats fine.
    case other =>
      context.system.log.debug("received unknown event to recover:" + other)
  }

  override def receiveCommand: Receive = {
    case evt: DomainEvent => {
      if (tags.isEmpty) {
        persist(evt) { e =>
          context.system.log.debug(s"persisted $e")
          sender() ! e
        }
      } else {
        persist(Tagged(evt, tags)) { e =>
          context.system.log.debug(s"persisted $e")
          val Tagged(event, _) = e
          sender() ! event
        }
      }
    }
    case other => context.system.log.error(s"cannot handle command $other")
  }

}
