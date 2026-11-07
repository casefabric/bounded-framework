/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.cluster

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.serialization.{SerializationExtension, Serializers}
import io.cafienne.bounded.aggregate.{
  EventPosition,
  Persisted,
  Ko,
  Ok,
  PersistenceFailed,
  PersistenceRejected,
  RemoteFailureCause,
  ReplySerializer,
  UnexpectedCommand
}
import io.cafienne.bounded.cluster.Notes._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * The replies of aggregates and Bounded's failures serialize without Java serialization, with the application's own
  * serializer for what they hold.
  */
class ReplySerializerSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll {

  private val system = ActorSystem[Nothing](
    Behaviors.empty[Nothing],
    "ReplySerializerSpec",
    ClusterNodes.config("ReplySerializerSpec", "pekko.actor.provider = local")
  )
  private val serialization = SerializationExtension(system)

  private def roundTrip(message: AnyRef): AnyRef = {
    val serializer = serialization.findSerializerFor(message)
    serializer shouldBe a[ReplySerializer]
    val bytes = serializer.toBinary(message)
    serialization.deserialize(bytes, serializer.identifier, Serializers.manifestFor(serializer, message)).get
  }

  "The ReplySerializer" should {
    "serialize an Ok with the events, using their own serializer" in {
      val ok = Ok(List(NoteAdded("a", "first"), NoteAdded("a", "second")))
      roundTrip(ok) shouldBe ok
      roundTrip(Ok(Nil)) shouldBe Ok(Nil)
    }

    "serialize a Persisted with the events and the position of the last one" in {
      val persisted = Persisted(List(NoteAdded("a", "first"), NoteAdded("a", "second")), EventPosition("notes|a", 7))
      roundTrip(persisted) shouldBe persisted
    }

    "serialize a Ko with an application failure" in {
      roundTrip(Ko(NoteRefused("no"))) shouldBe Ko(NoteRefused("no"))
    }

    "serialize Bounded's failures, keeping the class name and message of the cause" in {
      roundTrip(Ko(PersistenceFailed(new IllegalStateException("journal down")))) match {
        case Ko(PersistenceFailed(cause: RemoteFailureCause)) =>
          cause.className shouldBe classOf[IllegalStateException].getName
          cause.originalMessage shouldBe "journal down"
          cause.getMessage shouldBe "java.lang.IllegalStateException: journal down"
        case other => fail(s"Unexpected $other")
      }
      roundTrip(PersistenceRejected(new RuntimeException())) match {
        case PersistenceRejected(cause: RemoteFailureCause) => cause.originalMessage shouldBe ""
        case other                                          => fail(s"Unexpected $other")
      }
      // a cause that crossed nodes before crosses again unchanged
      val remote = PersistenceFailed(new RemoteFailureCause("x.Failure", "it failed"))
      roundTrip(remote) match {
        case PersistenceFailed(cause: RemoteFailureCause) =>
          (cause.className, cause.originalMessage) shouldBe ("x.Failure", "it failed")
        case other => fail(s"Unexpected $other")
      }
    }

    "serialize an UnexpectedCommand with the command" in {
      val replyTo = system.systemActorOf(Behaviors.empty[NoteList], "reply-to")
      val command = GetNotes("a", replyTo)
      roundTrip(new UnexpectedCommand(command)) match {
        case unexpected: UnexpectedCommand => unexpected.command shouldBe command
        case other                         => fail(s"Unexpected $other")
      }
    }
  }

  override protected def afterAll(): Unit = {
    system.terminate()
    Await.result(system.whenTerminated, 30.seconds): Unit
  }
}
