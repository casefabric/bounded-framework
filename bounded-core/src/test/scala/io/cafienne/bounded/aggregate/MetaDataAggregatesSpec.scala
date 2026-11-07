/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.aggregate

import org.apache.pekko.actor.{ExtendedActorSystem, Props}
import org.apache.pekko.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.actor.typed.scaladsl.adapter._
import org.apache.pekko.cluster.sharding.typed.scaladsl.EntityTypeKey
import org.apache.pekko.persistence.testkit.PersistenceTestKitPlugin
import org.apache.pekko.persistence.testkit.scaladsl.PersistenceTestKit
import org.apache.pekko.persistence.typed.PersistenceId
import org.apache.pekko.persistence.typed.scaladsl.Effect
import org.apache.pekko.testkit.TestProbe
import com.typesafe.config.ConfigFactory
import io.cafienne.bounded.aggregate.typed.{AggregateBehavior, TypedAggregateRootManager}
import org.scalatest.wordspec.AnyWordSpecLike

/** A classic and a typed aggregate that pass the metadata of their commands on to their events. */
object MetaDataAggregatesSpec {
  val Type: AggregateType = AggregateType("ticket")

  final case class OpenTicket(aggregateRootId: String, title: String, metaData: CommandMetaData)
      extends DomainCommand
      with HasCommandMetaData
  final case class TicketOpened(id: String, title: String, metaData: EventMetaData)
      extends DomainEvent
      with HasEventMetaData

  final case class TicketState(title: String) extends AggregateState[TicketState] {
    override def update(event: DomainEvent): Option[TicketState] = Some(this)
  }

  class ClassicTicket(id: String) extends AggregateRootActor[TicketState] {
    override def aggregateId: String                                                      = id
    override def aggregateType: AggregateType                                             = Type
    override def handleCommand(command: DomainCommand, state: Option[TicketState]): Reply = command match {
      case open: OpenTicket => Ok(List(TicketOpened(id, open.title, EventMetaData.causedBy(open.metaData))))
      case other            => Ko(new UnexpectedCommand(other))
    }
    override def newState(event: DomainEvent): Option[TicketState] = event match {
      case opened: TicketOpened => Some(TicketState(opened.title))
      case _                    => None
    }
  }

  final case class TypedOpenTicket(
    aggregateRootId: String,
    title: String,
    metaData: CommandMetaData,
    replyTo: ActorRef[Reply]
  ) extends DomainCommand
      with HasCommandMetaData

  class TypedTicketManager extends TypedAggregateRootManager[TypedOpenTicket] {
    override def entityTypeKey: EntityTypeKey[TypedOpenTicket] = EntityTypeKey[TypedOpenTicket](Type.name)

    override def behavior(persistenceId: PersistenceId): Behavior[TypedOpenTicket] =
      AggregateBehavior.withEnforcedReplies[TypedOpenTicket, TicketOpened, Option[String]](
        persistenceId,
        None,
        (_, command) => {
          val event =
            TicketOpened(command.aggregateRootId, command.title, EventMetaData.causedBy(command.metaData))
          Effect.persist(event).thenReply(command.replyTo)(_ => Ok(List(event)))
        },
        (_, event) => Some(event.title)
      )
  }
}

class MetaDataAggregatesSpec
    extends ScalaTestWithActorTestKit(
      PersistenceTestKitPlugin.config.withFallback(
        ConfigFactory.parseString("""
          pekko.actor.allow-java-serialization = on
          pekko.actor.warn-about-java-serializer-usage = off
        """)
      )
    )
    with AnyWordSpecLike {
  import MetaDataAggregatesSpec._

  private val classic     = system.toClassic
  private val persistence = PersistenceTestKit(system)

  private def stored(persistenceId: String): List[TicketOpened] =
    persistence.persistedInStorage(persistenceId).toList.map(_.asInstanceOf[TicketOpened])

  "A classic aggregate" should {
    "store the metadata of its command in its event" in {
      val command   = OpenTicket("t1", "printer", CommandMetaData.create("request-1", "user-42", Map("tenant" -> "a")))
      val probe     = TestProbe()(classic)
      val aggregate =
        classic.asInstanceOf[ExtendedActorSystem].systemActorOf(Props(new ClassicTicket("t1")), "classic-ticket")

      probe.send(aggregate, command)
      probe.expectMsgType[Ok]

      val event = stored("ticket|t1").head
      event.metaData.correlationId shouldBe "request-1"
      event.metaData.causationId shouldBe Some(command.metaData.commandId.toString)
      event.metaData.actor shouldBe "user-42"
      event.metaData.fields shouldBe Map("tenant" -> "a")
      EventMetaData.of(event) shouldBe Some(event.metaData)
    }
  }

  "A typed aggregate" should {
    "store the metadata of its command in its event" in {
      val meta      = CommandMetaData.create("request-2", "user-7")
      val probe     = createTestProbe[Reply]()
      val aggregate = spawn(new TypedTicketManager().behaviorFor("t2"))

      aggregate ! TypedOpenTicket("t2", "screen", meta, probe.ref)
      probe.expectMessageType[Ok]

      val event = stored("ticket|t2").head
      event.metaData.correlationId shouldBe "request-2"
      event.metaData.causationId shouldBe Some(meta.commandId.toString)
      // a policy reacting to the event continues the chain
      val next = CommandMetaData.causedBy(event.metaData, actor = Some("system:policy"))
      next.correlationId shouldBe "request-2"
      next.causationId shouldBe Some(event.metaData.eventId.toString)
    }
  }
}
