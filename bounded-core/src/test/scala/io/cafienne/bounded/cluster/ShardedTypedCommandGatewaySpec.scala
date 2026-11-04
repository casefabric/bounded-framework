/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.cluster

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.persistence.jdbc.query.scaladsl.JdbcReadJournal
import org.apache.pekko.persistence.query.PersistenceQuery
import org.apache.pekko.stream.scaladsl.Sink
import org.apache.pekko.util.Timeout
import io.cafienne.bounded.aggregate.{CommandValidationException, Ko, Ok, Reply, ValidateableCommand}
import io.cafienne.bounded.aggregate.typed.{ShardedTypedCommandGateway, TypedCommandGateway}
import io.cafienne.bounded.cluster.Notes._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

/**
  * The sharded gateway on a cluster with Java serialization off: commands and Bounded's replies cross nodes, and one
  * aggregate runs on one node only, wherever its commands come from.
  */
class ShardedTypedCommandGatewaySpec
    extends AnyWordSpec
    with Matchers
    with ScalaFutures
    with Eventually
    with BeforeAndAfterAll {

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(30, Seconds), interval = Span(100, Millis))
  implicit val timeout: Timeout = Timeout(10.seconds)

  // Rejects the note "forbidden"; accepts everything else.
  implicit val validator: ValidateableCommand[NotesCommand] = {
    case AddNote(_, "forbidden", _) => Future.failed(CommandValidationException("forbidden note"))
    case command                    => Future.successful(command)
  }

  private var clusters = Vector.empty[Vector[ActorSystem[Nothing]]]

  private def startCluster(name: String, count: Int): Vector[ActorSystem[Nothing]] = {
    val nodes = ClusterNodes.start(name, count)
    clusters :+= nodes
    nodes
  }

  private def gatewayOn(node: ActorSystem[?]): ShardedTypedCommandGateway[NotesCommand] = {
    implicit val ec: ExecutionContext = node.executionContext
    new ShardedTypedCommandGateway[NotesCommand](node, new NotesManager())
  }

  private def add(gateway: TypedCommandGateway[NotesCommand], id: String, note: String): Future[Reply] =
    gateway.ask[Reply](id, AddNote(id, note, _))

  private def notes(gateway: TypedCommandGateway[NotesCommand], id: String): NoteList =
    gateway.ask[NoteList](id, GetNotes(id, _)).futureValue

  "A ShardedTypedCommandGateway on one node" should {
    "deliver asked and told commands, and run their validation" in {
      val node    = startCluster("ShardedGatewayOneNode", 1).head
      val gateway = gatewayOn(node)

      add(gateway, "one", "first").futureValue shouldBe Ok(List(NoteAdded("one", "first")))
      gateway.tell(AddNote("one", "second", node.ignoreRef)).futureValue
      eventually(notes(gateway, "one").notes shouldBe List("first", "second"))

      add(gateway, "one", "").futureValue shouldBe Ko(NoteRefused("a note cannot be empty"))
      add(gateway, "one", "forbidden").failed.futureValue shouldBe a[CommandValidationException]
      gateway.tell(AddNote("one", "forbidden", node.ignoreRef)).failed.futureValue shouldBe a[
        CommandValidationException
      ]
      notes(gateway, "one").notes shouldBe List("first", "second")
    }
  }

  "ShardedTypedCommandGateways on two nodes" should {
    "run an aggregate on one node, whichever node its commands come from" in {
      val nodes                         = startCluster("ShardedGatewayTwoNodes", 2)
      val gateways                      = nodes.map(gatewayOn)
      implicit val ec: ExecutionContext = nodes.head.executionContext

      // the commands of both nodes arrive interleaved; two actors on one stream would conflict on sequence numbers
      val replies = Future
        .sequence((1 to 20).map(i => add(gateways(i % 2), "shared", s"note-$i")))
        .futureValue
      replies.foreach(_ shouldBe an[Ok])

      val fromFirst  = notes(gateways(0), "shared")
      val fromSecond = notes(gateways(1), "shared")
      fromFirst shouldBe fromSecond
      fromFirst.notes should contain theSameElementsAs (1 to 20).map(i => s"note-$i")

      val journal = PersistenceQuery(nodes.head).readJournalFor[JdbcReadJournal](JdbcReadJournal.Identifier)
      val stored  = journal
        .currentEventsByPersistenceId(Type.persistenceId("shared").id, 0L, Long.MaxValue)
        .runWith(Sink.seq)(org.apache.pekko.stream.Materializer(nodes.head))
        .futureValue
      stored.map(_.sequenceNr) shouldBe (1L to 20L)
      stored.map(_.event).toSet shouldBe fromFirst.notes.map(NoteAdded("shared", _)).toSet
    }

    "hand its aggregates over to the other node on shutdown" in {
      val nodes              = clusters.last
      val gateways           = nodes.map(gatewayOn)
      val running            = notes(gateways(0), "shared")
      val (leaving, staying) =
        if (running.node == org.apache.pekko.cluster.typed.Cluster(nodes(0)).selfMember.address.toString) (0, 1)
        else (1, 0)

      gateways(leaving).shutdown().futureValue

      val moved = notes(gateways(staying), "shared")
      moved.node should not be running.node
      moved.notes shouldBe running.notes
      add(gateways(staying), "shared", "after the move").futureValue shouldBe an[Ok]
    }
  }

  override protected def afterAll(): Unit = clusters.foreach(ClusterNodes.stop)
}
