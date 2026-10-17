/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

///*
// * Copyright (C) 2016-2023 Batav B.V. <https://www.cafienne.io/bounded>
// */
//
//package io.cafienne.bounded.eventmaterializers
//
//import org.apache.pekko.actor.{ActorContext, ActorRef}
//import org.apache.pekko.persistence.PersistentActor
//import org.apache.pekko.persistence.journal.Tagged
//import io.cafienne.bounded.aggregate.DomainEvent
//
//class CreateEventsInStoreActor(aggregateId: String) extends PersistentActor {
//  override def persistenceId: String = aggregateId
//
//  override def receiveRecover: Receive = {
//    case other =>
//      context.system.log.debug("received unknown event to recover:" + other)
//  }
//
//  override def receiveCommand: Receive = {
//    case evt: Tagged      => storeAndReply(sender(), evt)
//    case evt: DomainEvent => storeAndReply(sender(), evt)
//    case other            => context.system.log.error(s"cannot handle storage of event $other")
//  }
//
//  private def storeAndReply(replyTo: ActorRef, evt: Any)(implicit context: ActorContext): Unit = {
//    persist(evt) { e =>
//      context.system.log.debug(s"persisted $e")
//      replyTo ! e
//    }
//  }
//
//}
