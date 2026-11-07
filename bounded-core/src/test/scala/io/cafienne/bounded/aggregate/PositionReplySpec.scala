/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.aggregate

import org.apache.pekko.actor.{ExtendedActorSystem, Props}
import org.apache.pekko.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors}
import org.apache.pekko.actor.typed.scaladsl.adapter._
import org.apache.pekko.cluster.sharding.typed.scaladsl.EntityTypeKey
import org.apache.pekko.persistence.testkit.PersistenceTestKitPlugin
import org.apache.pekko.persistence.typed.PersistenceId
import org.apache.pekko.persistence.typed.scaladsl.{Effect, ReplyEffect}
import org.apache.pekko.persistence.testkit.scaladsl.PersistenceTestKit
import org.apache.pekko.testkit.TestProbe
import com.typesafe.config.ConfigFactory
import io.cafienne.bounded.aggregate.typed.{AggregateBehavior, TypedAggregateRootManager}
import org.scalatest.wordspec.AnyWordSpecLike

object PositionReplySpec {
  val Type: AggregateType = AggregateType("counter")

  final case class Count(aggregateRootId: String, times: Int, replyTo: ActorRef[Reply]) extends DomainCommand
  final case class Counted(id: String)                                                  extends DomainEvent

  class CounterManager extends TypedAggregateRootManager[Count] {
    override def entityTypeKey: EntityTypeKey[Count] = EntityTypeKey[Count](Type.name)

    override def behavior(persistenceId: PersistenceId): Behavior[Count] =
      Behaviors.setup { context =>
        AggregateBehavior.withEnforcedReplies[Count, Counted, Int](
          persistenceId,
          0,
          (_, command) => handle(context, persistenceId, command),
          (count, _) => count + 1
        )
      }
  }

  /** The same aggregate, built with `AggregateBehavior.withContext`. */
  class CounterWithContextManager extends TypedAggregateRootManager[Count] {
    override def entityTypeKey: EntityTypeKey[Count] = EntityTypeKey[Count](Type.name)

    override def behavior(persistenceId: PersistenceId): Behavior[Count] =
      AggregateBehavior.withContext[Count, Counted, Int](
        persistenceId,
        0,
        (context, _, command) => handle(context, persistenceId, command),
        (count, _) => count + 1,
        _.withTagger(_ => Set(Type.name, "counted"))
      )
  }

  private def handle(
    context: ActorContext[Count],
    persistenceId: PersistenceId,
    command: Count
  ): ReplyEffect[Counted, Int] = {
    val events = List.fill(command.times)(Counted(command.aggregateRootId))
    if (events.isEmpty) Effect.reply(command.replyTo)(Ok(Nil))
    else
      Effect
        .persist(events)
        .thenReply(command.replyTo)(_ => Persisted(events, AggregateBehavior.positionOf(context, persistenceId)))
  }
}

class PositionReplySpec
    extends ScalaTestWithActorTestKit(
      PersistenceTestKitPlugin.config.withFallback(
        ConfigFactory.parseString("pekko.actor.allow-java-serialization = on")
      )
    )
    with AnyWordSpecLike {
  import ClassicSimpleAggregate._
  import PositionReplySpec._

  private val classic = system.toClassic

  private def actorOf(props: Props) =
    classic.asInstanceOf[ExtendedActorSystem].systemActorOf(props, props.hashCode().toString)

  "A classic aggregate with replyWithPosition" should {
    "reply Persisted with the persistence id and sequence number of the last event" in {
      val probe     = TestProbe()(classic)
      val aggregate = actorOf(Props(new ClassicSimpleAggregate("ar-pos") {
        override val replyWithPosition: Boolean = true
      }))

      probe.send(aggregate, Create("ar-pos"))
      probe.expectMsg(Persisted(List(Created("ar-pos")), EventPosition("simple|ar-pos", 1)))
      probe.send(aggregate, AddItems("ar-pos", List("a", "b")))
      probe.expectMsg(
        Persisted(List(ItemAdded("ar-pos", "a"), ItemAdded("ar-pos", "b")), EventPosition("simple|ar-pos", 3))
      )
      probe.send(aggregate, Stop("ar-pos"))
      probe.expectMsg(Ok(Nil))
    }
  }

  "A classic aggregate without replyWithPosition" should {
    "reply Ok as before" in {
      val probe     = TestProbe()(classic)
      val aggregate = actorOf(Props(new ClassicSimpleAggregate("ar-plain")))
      probe.send(aggregate, Create("ar-plain"))
      probe.expectMsg(Ok(List(Created("ar-plain"))))
    }
  }

  "AggregateBehavior.positionOf" should {
    "give the position of the last persisted event in thenReply" in {
      val aggregate = spawn(new CounterManager().behaviorFor("c1"))
      val probe     = createTestProbe[Reply]()

      aggregate ! Count("c1", 2, probe.ref)
      probe.expectMessage(Persisted(List(Counted("c1"), Counted("c1")), EventPosition("counter|c1", 2)))
      aggregate ! Count("c1", 1, probe.ref)
      probe.expectMessage(Persisted(List(Counted("c1")), EventPosition("counter|c1", 3)))
      aggregate ! Count("c1", 0, probe.ref)
      probe.expectMessage(Ok(Nil))
    }

    "work with withContext, and apply its adjust to the EventSourcedBehavior" in {
      val aggregate = spawn(new CounterWithContextManager().behaviorFor("c2"))
      val probe     = createTestProbe[Reply]()

      aggregate ! Count("c2", 2, probe.ref)
      probe.expectMessage(Persisted(List(Counted("c2"), Counted("c2")), EventPosition("counter|c2", 2)))
      PersistenceTestKit(system).persistedInStorage("counter|c2") shouldBe List(Counted("c2"), Counted("c2"))
    }
  }
}
