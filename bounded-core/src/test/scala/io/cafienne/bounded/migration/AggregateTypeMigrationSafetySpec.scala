/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.migration

import org.apache.pekko.Done
import org.apache.pekko.actor.{ActorSystem, Props}
import org.apache.pekko.pattern.ask
import org.apache.pekko.persistence.query.{NoOffset, PersistenceQuery}
import org.apache.pekko.persistence.query.scaladsl.{
  CurrentEventsByPersistenceIdQuery,
  CurrentEventsByTagQuery,
  CurrentPersistenceIdsQuery,
  ReadJournal
}
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.testkit.{TestKit, TestProbe}
import org.apache.pekko.util.Timeout
import com.typesafe.config.ConfigFactory
import io.cafienne.bounded.aggregate.{AggregateType, ClassicSimpleAggregate, Ko}
import io.cafienne.bounded.aggregate.ClassicSimpleAggregate._
import io.cafienne.bounded.eventmaterializers.SpecConfig
import io.github.alstanchev.pekko.persistence.inmemory.query.scaladsl.InMemoryReadJournal
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration._

/**
  * The cases in which AggregateTypeMigration must refuse, report or work around something, each on a journal of its
  * own (the in-memory journal, without a snapshot store).
  */
class AggregateTypeMigrationSafetySpec extends AnyWordSpec with Matchers with ScalaFutures {
  import AggregateTypeMigrationSpec._

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(30, Seconds), interval = Span(100, Millis))
  implicit val timeout: Timeout = Timeout(10.seconds)

  private val systems = new AtomicInteger()

  private val noSnapshotStore = ConfigFactory
    .parseString("""pekko.persistence.snapshot-store.plugin = """"")
    .withFallback(SpecConfig.testConfig)

  private def withJournal(test: ActorSystem => Any): Unit = {
    val system = ActorSystem(s"AggregateTypeMigrationSafetySpec-${systems.incrementAndGet()}", noSnapshotStore)
    try { test(system); () }
    finally TestKit.shutdownActorSystem(system)
  }

  "AggregateTypeMigration" should {

    "delete the old streams when no snapshot store is configured" in withJournal { implicit system =>
      legacyUnder("p", Append(Seq(Created("p"), ItemAdded("p", "p1"))))
      val migration = new AggregateTypeMigration(system, rule)

      val migrated = migration.migrate().futureValue
      migrated.verified shouldBe true
      migration.deleteMigrated(migrated).futureValue shouldBe Done

      events("p") shouldBe empty
      classicItems("p") shouldBe Seq("p1")
    }

    "copy nothing when two old streams have the same target" in withJournal { implicit system =>
      legacyUnder("x1", Append(Seq(Created("x"))))
      legacyUnder("x2", Append(Seq(ItemAdded("x", "x1"))))
      val sameTarget: (String, Any) => Option[MigrationTarget] = (_, _) => Some(MigrationTarget(Simple, "x"))

      val migrated = new AggregateTypeMigration(system, sameTarget).migrate().futureValue

      migrated.verified shouldBe false
      migrated.conflicts shouldBe Seq("simple|x is the target of x1, x2")
      persistenceIds() should not contain "simple|x"
    }

    "copy nothing when a target is an old stream that is migrated itself" in withJournal { implicit system =>
      legacyUnder("q", Append(Seq(Created("q"))))
      // a 1.0.0 stream under the target's id that the rule moves to another type
      legacyUnder("simple|q", Append(Seq(TypedSimpleAggregateCreated("q"))))

      val migrated = new AggregateTypeMigration(system, rule).migrate().futureValue

      migrated.verified shouldBe false
      migrated.conflicts shouldBe Seq("simple|q is a target of q, but is migrated itself")
      events("simple|q") shouldBe Seq(TypedSimpleAggregateCreated("q"))
      persistenceIds() should not contain s"${Typed.name}|q"
    }

    "fail when a target already holds an event that is not a copy" in withJournal { implicit system =>
      legacyUnder("g", Append(Seq(Created("g"), ItemAdded("g", "g1"))))
      // written by the new version of the application before migrating
      legacyUnder("simple|g", Append(Seq(Created("g")), tags = Set(Simple.name)))
      legacyUnder("simple|g", Append(Seq(ItemAdded("g", "new")), tags = Set(Simple.name)))

      val failure = new AggregateTypeMigration(system, rule).migrate().failed.futureValue

      failure shouldBe an[IllegalStateException]
      failure.getMessage should include("simple|g already holds an event 2")
      events("g") should have size 2
    }

    "fail when a target holds more events than its old stream gives it" in withJournal { implicit system =>
      legacyUnder("h", Append(Seq(Created("h"))))
      legacyUnder("simple|h", Append(Seq(Created("h"), ItemAdded("h", "new")), tags = Set(Simple.name)))

      val failure = new AggregateTypeMigration(system, rule).migrate().failed.futureValue

      failure.getMessage should include("simple|h already holds 2 events, more than the 1")
    }

    "migrate 1.0.0 streams with a prefix, and report those in place whose events lack the type tag" in withJournal {
      implicit system =>
        // a classic aggregate with persistenceIdPrefix "Simple|": another name than the type
        legacyUnder("Simple|k", Append(Seq(Created("k"))))
        // prefix equal to the type, events not tagged with it
        legacyUnder("simple|m", Append(Seq(Created("m"), ItemAdded("m", "m1"))))
        // prefix equal to the type, events tagged with it
        legacyUnder("simple|t", Append(Seq(Created("t")), tags = Set(Simple.name)))
        val migration = new AggregateTypeMigration(system, rule)

        val dryRun = migration.dryRun().futureValue
        val byId   = dryRun.streams.map(stream => stream.persistenceId -> stream).toMap
        byId("Simple|k").migrates shouldBe true
        byId("Simple|k").targets.keySet shouldBe Set("simple|k")
        byId("simple|m").inPlace shouldBe true
        byId("simple|m").untagged shouldBe 2L
        byId("simple|t").inPlace shouldBe true
        byId("simple|t").untagged shouldBe 0L
        dryRun.untagged.map(_.persistenceId) shouldBe Seq("simple|m")

        val migrated = migration.migrate().futureValue
        migrated.verified shouldBe true
        classicItems("k") shouldBe Nil
        events("simple|k") shouldBe Seq(Created("k"))
    }

    "add the extra tags a rule gives" in withJournal { implicit system =>
      legacyUnder("e", Append(Seq(Created("e"), ItemAdded("e", "e1"))))
      val tagged = AggregateTypeMigration.withTags(rule) {
        case _: ItemAdded => Set("items")
        case _            => Set.empty
      }

      new AggregateTypeMigration(system, tagged).migrate().futureValue.verified shouldBe true

      eventsByTag("items") shouldBe Seq("simple|e" -> ItemAdded("e", "e1"))
      eventsByTag(Simple.name).map(_._1) shouldBe Seq("simple|e", "simple|e")
    }

    "keep aggregate ids that contain the separator" in withJournal { implicit system =>
      val persistenceId = Simple.persistenceId("customer|42")
      persistenceId.id shouldBe "simple|customer|42"
      AggregateType.aggregateIdOf(persistenceId) shouldBe "customer|42"
      persistenceId.entityTypeHint shouldBe Simple.name
      AggregateType.of(persistenceId.id) shouldBe Some(Simple)

      val aggregate = system.actorOf(Props(classOf[ClassicSimpleAggregate], "customer|42"))
      (aggregate ? Create("customer|42")).futureValue shouldBe a[io.cafienne.bounded.aggregate.Ok]
      system.stop(aggregate)
      events("simple|customer|42") shouldBe Seq(Created("customer|42"))
    }
  }

  private def TypedSimpleAggregateCreated(id: String) =
    io.cafienne.bounded.aggregate.TypedSimpleAggregate.Created(id, meta)

  private def legacyUnder(persistenceId: String, command: Any)(implicit system: ActorSystem): Unit = {
    val probe  = TestProbe()
    val stream = system.actorOf(Props(new LegacyStream(persistenceId)))
    probe.send(stream, command)
    probe.expectMsg(10.seconds, Done)
    probe.watch(stream)
    system.stop(stream)
    probe.expectTerminated(stream)
    ()
  }

  private def readJournal(
    implicit system: ActorSystem
  ): ReadJournal with CurrentPersistenceIdsQuery with CurrentEventsByPersistenceIdQuery with CurrentEventsByTagQuery =
    PersistenceQuery(system).readJournalFor[InMemoryReadJournal](InMemoryReadJournal.Identifier)

  private def persistenceIds()(implicit system: ActorSystem): Seq[String] =
    readJournal.currentPersistenceIds().runWith(Sink.seq).futureValue

  private def events(persistenceId: String)(implicit system: ActorSystem): Seq[Any] =
    readJournal
      .currentEventsByPersistenceId(persistenceId, 0L, Long.MaxValue)
      .map(_.event)
      .runWith(Sink.seq)
      .futureValue

  private def eventsByTag(tag: String)(implicit system: ActorSystem): Seq[(String, Any)] =
    readJournal
      .currentEventsByTag(tag, NoOffset)
      .map(envelope => envelope.persistenceId -> envelope.event)
      .runWith(Sink.seq)
      .futureValue

  private def classicItems(id: String)(implicit system: ActorSystem): Seq[String] = {
    val aggregate = system.actorOf(Props(classOf[ClassicSimpleAggregate], id))
    try (aggregate ? GetItems(id)).mapTo[Ko].futureValue.failure.asInstanceOf[Items].items
    finally system.stop(aggregate)
  }
}
