/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.aggregate

import org.apache.pekko.actor.{ActorSystem, Props}
import org.apache.pekko.persistence.testkit.PersistenceTestKitPlugin
import org.apache.pekko.persistence.testkit.scaladsl.PersistenceTestKit
import org.apache.pekko.testkit.{ImplicitSender, TestKit, TestProbe}
import com.typesafe.config.ConfigFactory
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

class AggregateRootActorSpec
    extends TestKit(
      ActorSystem(
        "AggregateRootActorSpec",
        PersistenceTestKitPlugin.config
          .withFallback(ConfigFactory.parseString("""
    pekko.actor.warn-about-java-serializer-usage = off
    pekko.coordinated-shutdown.terminate-actor-system = off
    pekko.coordinated-shutdown.run-by-actor-system-terminate = off
    pekko.coordinated-shutdown.run-by-jvm-shutdown-hook = off
    """))
      )
    )
    with ImplicitSender
    with AnyFlatSpecLike
    with Matchers
    with BeforeAndAfterEach
    with BeforeAndAfterAll {

  import ClassicSimpleAggregate._

  val persistenceTestKit: PersistenceTestKit = PersistenceTestKit(system)
  val creator                                = new ClassicSimpleAggregateCreator(system)

  override protected def beforeEach(): Unit = {
    persistenceTestKit.clearAll()
    persistenceTestKit.resetPolicy()
  }

  behavior of "Classic AggregateRootActor"

  it should "reply Ok after the events are stored" in {
    val aggregateRef = system.actorOf(creator.props("ar-ok"))

    aggregateRef ! Create("ar-ok")

    expectMsg(Ok(List(Created("ar-ok"))))
    persistenceTestKit.expectNextPersisted("simple|ar-ok", Created("ar-ok"))
  }

  it should "reply Ok without events when the command produces none" in {
    val aggregateRef = system.actorOf(creator.props("ar-empty"))
    val watcher      = TestProbe()
    watcher.watch(aggregateRef)

    aggregateRef ! Stop("ar-empty")

    expectMsg(Ok(List.empty))
    watcher.expectTerminated(aggregateRef)
  }

  it should "reply Ko and stop when persisting fails" in {
    val aggregateRef = system.actorOf(creator.props("ar-fail"))
    val watcher      = TestProbe()
    watcher.watch(aggregateRef)
    persistenceTestKit.failNextPersisted("simple|ar-fail", new RuntimeException("journal down"))

    aggregateRef ! Create("ar-fail")

    expectMsgPF() { case Ko(PersistenceFailed(cause)) => cause.getMessage shouldBe "journal down" }
    watcher.expectTerminated(aggregateRef)
    persistenceTestKit.expectNothingPersisted("simple|ar-fail")
  }

  it should "reply Ko and keep running when the journal rejects the events" in {
    val aggregateRef = system.actorOf(creator.props("ar-reject"))
    persistenceTestKit.rejectNextPersisted("simple|ar-reject", new RuntimeException("rejected"))

    aggregateRef ! Create("ar-reject")

    expectMsgPF() { case Ko(PersistenceRejected(cause)) => cause.getMessage shouldBe "rejected" }
    persistenceTestKit.expectNothingPersisted("simple|ar-reject")

    aggregateRef ! Create("ar-reject")

    expectMsg(Ok(List(Created("ar-reject"))))
    persistenceTestKit.expectNextPersisted("simple|ar-reject", Created("ar-reject"))
  }

  it should "reply once, after the last event, when a command produces several events" in {
    val aggregateRef = system.actorOf(creator.props("ar-multi"))
    aggregateRef ! Create("ar-multi")
    expectMsg(Ok(List(Created("ar-multi"))))

    aggregateRef ! AddItems("ar-multi", List("a", "b", "c"))

    expectMsg(Ok(List(ItemAdded("ar-multi", "a"), ItemAdded("ar-multi", "b"), ItemAdded("ar-multi", "c"))))
    expectNoMessage(200.millis)
    aggregateRef ! GetItems("ar-multi")
    expectMsg(Ko(Items(List("a", "b", "c"))))
  }

  it should "use the aggregate type and the aggregate id as persistence id" in {
    val aggregateRef = system.actorOf(creator.props("ar-type"))

    aggregateRef ! AggregateRootActor.GetPersistenceId

    expectMsg("simple|ar-type")
  }

  it should "keep aggregate types with the same id apart through their persistence id" in {
    import AggregateRootActorSpec._
    val id      = "ar-shared-id"
    val order   = system.actorOf(Props(classOf[OrderAggregate], id))
    val invoice = system.actorOf(Props(classOf[InvoiceAggregate], id))

    order ! Create(id)
    expectMsg(Ok(List(Created(id))))
    order ! AddItem(id, "order item")
    expectMsgType[Ok]
    invoice ! Create(id)
    expectMsg(Ok(List(Created(id))))
    invoice ! AddItem(id, "invoice item")
    expectMsgType[Ok]
    persistenceTestKit.persistedInStorage(s"order|$id") should have size 2
    persistenceTestKit.persistedInStorage(s"invoice|$id") should have size 2

    val watcher = TestProbe()
    Seq(order, invoice).foreach { ref =>
      watcher.watch(ref)
      system.stop(ref)
      watcher.expectTerminated(ref)
    }
    system.actorOf(Props(classOf[OrderAggregate], id)) ! GetItems(id)
    expectMsg(Ko(Items(Seq("order item"))))
    system.actorOf(Props(classOf[InvoiceAggregate], id)) ! GetItems(id)
    expectMsg(Ko(Items(Seq("invoice item"))))
  }

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system, 30.seconds, verifySystemShutdown = true)
  }
}

object AggregateRootActorSpec {
  class OrderAggregate(id: String) extends ClassicSimpleAggregate(id) {
    override def aggregateType: AggregateType = AggregateType("order")
  }

  class InvoiceAggregate(id: String) extends ClassicSimpleAggregate(id) {
    override def aggregateType: AggregateType = AggregateType("invoice")
  }
}
