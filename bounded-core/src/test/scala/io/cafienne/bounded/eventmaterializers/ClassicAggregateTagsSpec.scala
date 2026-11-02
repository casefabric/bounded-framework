/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.eventmaterializers

import org.apache.pekko.Done
import org.apache.pekko.actor.{ActorSystem, ExtendedActorSystem, PoisonPill, Props}
import org.apache.pekko.actor.testkit.typed.scaladsl.{LogCapturing, ScalaTestWithActorTestKit}
import org.apache.pekko.persistence.query.{NoOffset, Offset, PersistenceQuery, Sequence}
import org.apache.pekko.stream.scaladsl.Sink
import io.github.alstanchev.pekko.persistence.inmemory.query.scaladsl.InMemoryReadJournal
import org.apache.pekko.testkit.TestProbe
import com.typesafe.scalalogging.Logger
import io.cafienne.bounded.aggregate.{DomainEvent, Ok}
import io.cafienne.bounded.aggregate.AggregateRootActor.GetState
import io.cafienne.bounded.aggregate.ClassicSimpleAggregate
import io.cafienne.bounded.aggregate.ClassicSimpleAggregate._
import io.cafienne.bounded.eventmaterializers.offsetstores.InMemoryBasedOffsetStore
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.slf4j.LoggerFactory

import java.util.concurrent.ConcurrentLinkedQueue
import scala.collection.immutable.Seq
import scala.concurrent.Future
import scala.jdk.CollectionConverters._

object ClassicAggregateTagsSpec {
  val ItemTag = "classic-items"

  /** Tags its ItemAdded events with an extra tag; every event also gets the aggregate type as tag. */
  class TaggingClassicAggregate(id: String) extends ClassicSimpleAggregate(id) {
    override def tags(event: DomainEvent): Set[String] = event match {
      case _: ItemAdded => Set(ItemTag)
      case _            => Set.empty
    }
  }
}

class ClassicAggregateTagsSpec
    extends ScalaTestWithActorTestKit(
      // no tagging event adapter: the tags come from the aggregate only
      SpecConfig.testConfig
        .withoutPath("inmemory-journal.event-adapters")
        .withoutPath("inmemory-journal.event-adapter-bindings")
    )
    with AnyWordSpecLike
    with Matchers
    with LogCapturing {

  import ClassicAggregateTagsSpec._

  implicit val classicSystem: ActorSystem = system.classicSystem

  private val aggregateId = "classic-tags-ar"

  private def startAggregate(name: String) =
    classicSystem
      .asInstanceOf[ExtendedActorSystem]
      .systemActorOf(Props(classOf[TaggingClassicAggregate], aggregateId), name)

  "A classic aggregate" must {

    "tag every event with its aggregate type and extra tags without a journal event adapter, and recover" in {
      val probe     = TestProbe()
      val aggregate = startAggregate("classic-tags-1")
      probe.send(aggregate, Create(aggregateId))
      probe.expectMsg(Ok(Seq(Created(aggregateId))))
      probe.send(aggregate, AddItems(aggregateId, Seq("one", "two")))
      probe.expectMsg(Ok(Seq(ItemAdded(aggregateId, "one"), ItemAdded(aggregateId, "two"))))

      val materializer = new CollectingMaterializer()
      materializer.replayEvents().futureValue shouldBe Sequence(3L)
      materializer.handled shouldBe Seq(
        Created(aggregateId),
        ItemAdded(aggregateId, "one"),
        ItemAdded(aggregateId, "two")
      )

      // extra tags are stored too, for readers that query the journal by tag themselves
      PersistenceQuery(classicSystem)
        .readJournalFor[InMemoryReadJournal](InMemoryReadJournal.Identifier)
        .currentEventsByTag(ItemTag, NoOffset)
        .map(_.event)
        .runWith(Sink.seq)
        .futureValue shouldBe Seq(ItemAdded(aggregateId, "one"), ItemAdded(aggregateId, "two"))

      probe.watch(aggregate)
      aggregate ! PoisonPill
      probe.expectTerminated(aggregate)
      val recovered = startAggregate("classic-tags-2")
      probe.send(recovered, GetState)
      probe.expectMsg(Some(ClassicSimpleAggregateState(List("one", "two"))))
    }
  }

  class CollectingMaterializer extends AbstractReplayableEventMaterializer(classicSystem) {
    override val logger: Logger           = Logger(LoggerFactory.getLogger(classOf[CollectingMaterializer]))
    override val aggregateType            = ClassicSimpleAggregate.Type
    override val matMappingName: String   = "classic-tags-view"
    val offsets: InMemoryBasedOffsetStore = new InMemoryBasedOffsetStore()
    private val handledEvents             = new ConcurrentLinkedQueue[Any]()

    def handled: Seq[Any] = handledEvents.asScala.toList

    override def handleEvent(evt: Any): Future[Done] = {
      handledEvents.add(evt)
      Future.successful(Done)
    }

    override def handleReplayEvent(evt: Any): Future[Done] = handleEvent(evt)

    override def saveOffset(viewIdentifier: String, offset: Offset): Future[Unit] =
      offsets.saveOffset(viewIdentifier, offset)

    override def getOffset(viewIdentifier: String): Future[Offset] = offsets.getOffset(viewIdentifier)
  }
}
