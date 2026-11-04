/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.cluster

import org.apache.pekko.Done
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.adapter._
import org.apache.pekko.cluster.typed.Cluster
import org.apache.pekko.persistence.query.Offset
import org.apache.pekko.util.Timeout
import com.typesafe.scalalogging.Logger
import io.cafienne.bounded.aggregate.{Ok, Reply, ValidateableCommand}
import io.cafienne.bounded.aggregate.typed.ShardedTypedCommandGateway
import io.cafienne.bounded.cluster.Notes._
import io.cafienne.bounded.eventmaterializers.{
  AbstractReplayableEventMaterializer,
  ClusteredEventMaterializers,
  CombinedLogicEventMaterializer
}
import io.cafienne.bounded.eventmaterializers.offsetstores.InMemoryBasedOffsetStore
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.LoggerFactory

import java.util.concurrent.ConcurrentLinkedQueue
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

object ClusteredEventMaterializersSpec {

  /** What the materializers of all nodes handled: view, node, note. */
  val handled = new ConcurrentLinkedQueue[(String, String, String)]()

  /** The offsets of all nodes, as in a store the nodes share. */
  val offsets = new InMemoryBasedOffsetStore()

  def record(view: String, node: String, evt: Any): Future[Done] = {
    evt match {
      case NoteAdded(_, note) => handled.add((view, node, note))
      case _                  => ()
    }
    Future.successful(Done)
  }

  class NotesView(system: ActorSystem[?], view: String) extends AbstractReplayableEventMaterializer(system.toClassic) {
    private val node                    = Cluster(system).selfMember.address.toString
    override val logger: Logger         = Logger(LoggerFactory.getLogger(classOf[NotesView]))
    override val aggregateType          = Notes.Type
    override val matMappingName: String = view

    override def handleEvent(evt: Any): Future[Done]       = record(view, node, evt)
    override def handleReplayEvent(evt: Any): Future[Done] = record(view, node, evt)

    override def saveOffset(viewIdentifier: String, offset: Offset): Future[Unit] =
      offsets.saveOffset(viewIdentifier, offset)
    override def getOffset(viewIdentifier: String): Future[Offset] = offsets.getOffset(viewIdentifier)
  }

  class NotesLogic(system: ActorSystem[?], view: String) extends CombinedLogicEventMaterializer(system.toClassic) {
    private val node                    = Cluster(system).selfMember.address.toString
    override val logger: Logger         = Logger(LoggerFactory.getLogger(classOf[NotesLogic]))
    override val aggregateTypes         = List(Notes.Type)
    override val matMappingName: String = view

    override def handleEvent(evt: Any): Future[Done] = record(view, node, evt)

    override def saveOffset(viewIdentifier: String, offset: Offset): Future[Unit] =
      offsets.saveOffset(viewIdentifier, offset)
    override def getOffset(viewIdentifier: String): Future[Offset] = offsets.getOffset(viewIdentifier)
  }
}

/**
  * ClusteredEventMaterializers on two nodes that share a journal and an offset store: each materializer runs on one
  * node, and resumes on the other from its stored offset when its node leaves.
  */
class ClusteredEventMaterializersSpec
    extends AnyWordSpec
    with Matchers
    with ScalaFutures
    with Eventually
    with BeforeAndAfterAll {
  import ClusteredEventMaterializersSpec._

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(60, Seconds), interval = Span(200, Millis))
  implicit val timeout: Timeout                             = Timeout(10.seconds)
  implicit val validator: ValidateableCommand[NotesCommand] = Future.successful(_)

  private val views = List("notes-view", "notes-logic")
  private val nodes =
    ClusterNodes.start(
      "ClusteredMaterializers",
      2,
      "pekko.cluster.sharded-daemon-process.keep-alive-interval = 1s"
    )

  private def gatewayOn(node: ActorSystem[?]): ShardedTypedCommandGateway[NotesCommand] = {
    implicit val ec: ExecutionContext = node.executionContext
    new ShardedTypedCommandGateway[NotesCommand](node, new NotesManager())
  }

  private val gateways = nodes.map(gatewayOn)

  private def add(gateway: ShardedTypedCommandGateway[NotesCommand], notes: Seq[String]): Unit =
    notes.foreach(note =>
      gateway.ask[Reply](s"aggregate-$note", AddNote(s"aggregate-$note", note, _)).futureValue shouldBe an[Ok]
    )

  private def handledBy(view: String): Vector[(String, String)] =
    handled.asScala.toVector.collect { case (`view`, node, note) => (node, note) }

  private def address(node: ActorSystem[?]): String = Cluster(node).selfMember.address.toString

  "ClusteredEventMaterializers" should {
    "run every materializer on one node of the cluster" in {
      // the nodes give the materializers in a different order
      new ClusteredEventMaterializers(
        nodes(0),
        List(new NotesView(nodes(0), views(0)), new NotesLogic(nodes(0), views(1)))
      )
        .start()
      new ClusteredEventMaterializers(
        nodes(1),
        List(new NotesLogic(nodes(1), views(1)), new NotesView(nodes(1), views(0)))
      )
        .start()

      val first = (1 to 5).map(i => s"before-$i")
      add(gateways(0), first)

      views.foreach { view =>
        eventually(handledBy(view).map(_._2) should contain theSameElementsAs first)
        handledBy(view).map(_._1).distinct should have size 1
      }
    }

    "resume a materializer on another node from its stored offset when its node leaves" in {
      val viewNode           = handledBy(views(0)).head._1
      val (leaving, staying) = if (address(nodes(0)) == viewNode) (0, 1) else (1, 0)

      ClusterNodes.stop(Seq(nodes(leaving)))

      val second = (1 to 5).map(i => s"after-$i")
      add(gateways(staying), second)

      views.foreach { view =>
        eventually(handledBy(view).map(_._2) should contain allElementsOf second)
        handledBy(view).filter(_._2.startsWith("after")).map(_._1).distinct shouldBe Vector(address(nodes(staying)))
        // resumed from the stored offset instead of starting over
        handledBy(view).count(_._2 == "before-1") shouldBe 1
      }
    }
  }

  override protected def afterAll(): Unit = ClusterNodes.stop(nodes.filterNot(_.whenTerminated.isCompleted))
}
