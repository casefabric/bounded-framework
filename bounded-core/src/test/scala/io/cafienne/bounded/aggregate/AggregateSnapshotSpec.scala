/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.aggregate

import org.apache.pekko.actor.{ActorRef, ActorSystem, PoisonPill, Props, UnhandledMessage}
import org.apache.pekko.persistence.testkit.{PersistenceTestKitPlugin, PersistenceTestKitSnapshotPlugin}
import org.apache.pekko.persistence.testkit.scaladsl.{PersistenceTestKit, SnapshotTestKit}
import org.apache.pekko.testkit.{ImplicitSender, TestKit, TestProbe}
import com.typesafe.config.ConfigFactory
import io.cafienne.bounded.aggregate.AggregateRootActor.GetState
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.Seq
import scala.concurrent.duration._

object AggregateSnapshotSpec {
  class SnapshottingAggregate(id: String) extends ClassicSimpleAggregate(id) {
    override val snapShotInterval: Option[Int] = Some(2)
  }

  class KeepAllSnapshotsAggregate(id: String) extends SnapshottingAggregate(id) {
    override val deleteOlderSnapshots: Boolean = false
  }
}

class AggregateSnapshotSpec
    extends TestKit(
      ActorSystem(
        "AggregateSnapshotSpec",
        PersistenceTestKitPlugin.config
          .withFallback(PersistenceTestKitSnapshotPlugin.config)
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

  import AggregateSnapshotSpec._
  import ClassicSimpleAggregate._

  val persistenceTestKit: PersistenceTestKit = PersistenceTestKit(system)
  val snapshotTestKit: SnapshotTestKit       = SnapshotTestKit(system)

  override protected def beforeEach(): Unit = {
    persistenceTestKit.clearAll()
    persistenceTestKit.resetPolicy()
    snapshotTestKit.clearAll()
    snapshotTestKit.resetPolicy()
  }

  override protected def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  private def unhandledProbe(): TestProbe = {
    val probe = TestProbe()
    system.eventStream.subscribe(probe.ref, classOf[UnhandledMessage])
    probe
  }

  // Create plus 5 items: 6 events, snapshots at 2, 4 and 6
  private def sixEvents(aggregate: ActorRef, id: String): Unit = {
    aggregate ! Create(id)
    expectMsgType[Ok]
    (1 to 5).foreach { i =>
      aggregate ! AddItem(id, s"item-$i")
      expectMsgType[Ok]
    }
  }

  private def snapshotSequenceNrs(id: String): Seq[Long] =
    snapshotTestKit.persistedInStorage(id).map(_._1.sequenceNr).toList

  behavior of "Classic AggregateRootActor snapshots"

  it should "keep only the latest snapshot, without unhandled messages" in {
    val unhandled = unhandledProbe()
    val aggregate = system.actorOf(Props(classOf[SnapshottingAggregate], "snap-latest"))

    sixEvents(aggregate, "snap-latest")

    awaitAssert(snapshotSequenceNrs("snap-latest") shouldBe Seq(6L), 3.seconds)
    unhandled.expectNoMessage(300.millis)
  }

  it should "recover from the latest snapshot" in {
    val aggregate = system.actorOf(Props(classOf[SnapshottingAggregate], "snap-recover"))
    sixEvents(aggregate, "snap-recover")
    awaitAssert(snapshotSequenceNrs("snap-recover") shouldBe Seq(6L), 3.seconds)

    watch(aggregate)
    aggregate ! PoisonPill
    expectTerminated(aggregate)
    val recovered = system.actorOf(Props(classOf[SnapshottingAggregate], "snap-recover"))

    recovered ! GetState
    expectMsg(Some(ClassicSimpleAggregateState((1 to 5).map(i => s"item-$i").toList)))
  }

  it should "keep all snapshots when deleting older ones is turned off" in {
    val aggregate = system.actorOf(Props(classOf[KeepAllSnapshotsAggregate], "snap-keep"))

    sixEvents(aggregate, "snap-keep")

    awaitAssert(snapshotSequenceNrs("snap-keep") shouldBe Seq(2L, 4L, 6L), 3.seconds)
  }

  it should "keep running when saving a snapshot fails" in {
    val unhandled = unhandledProbe()
    snapshotTestKit.failNextPersisted()
    val aggregate = system.actorOf(Props(classOf[SnapshottingAggregate], "snap-save-fails"))

    sixEvents(aggregate, "snap-save-fails")

    awaitAssert(snapshotSequenceNrs("snap-save-fails") shouldBe Seq(6L), 3.seconds)
    unhandled.expectNoMessage(300.millis)
    aggregate ! GetState
    expectMsg(Some(ClassicSimpleAggregateState((1 to 5).map(i => s"item-$i").toList)))
  }

  it should "keep running when deleting older snapshots fails" in {
    val unhandled = unhandledProbe()
    snapshotTestKit.failNextDelete()
    val aggregate = system.actorOf(Props(classOf[SnapshottingAggregate], "snap-delete-fails"))

    sixEvents(aggregate, "snap-delete-fails")

    // the delete after snapshot 2 fails, the later ones still remove the older snapshots
    awaitAssert(snapshotSequenceNrs("snap-delete-fails") shouldBe Seq(6L), 3.seconds)
    unhandled.expectNoMessage(300.millis)
  }
}
