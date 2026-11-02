/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.test

import org.apache.pekko.persistence.{PersistentActor, RecoveryCompleted}
import org.apache.pekko.persistence.journal.Tagged
import io.cafienne.bounded.aggregate.DomainEvent

/**
  * Stores the events it receives under the given persistence id, tagged with the given tags, and replies with each
  * stored event.
  */
class CreateEventsInStoreActor(storeUnder: String, tags: Set[String] = Set.empty) extends PersistentActor {
  override def persistenceId: String = storeUnder

  override def receiveRecover: Receive = {
    case RecoveryCompleted => // thats fine.
    case other             =>
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
