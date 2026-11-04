/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.cluster

import org.apache.pekko.actor.ExtendedActorSystem
import org.apache.pekko.actor.typed.{ActorRef, ActorRefResolver, Behavior}
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.scaladsl.adapter._
import org.apache.pekko.cluster.sharding.typed.scaladsl.EntityTypeKey
import org.apache.pekko.cluster.typed.Cluster
import org.apache.pekko.persistence.typed.PersistenceId
import org.apache.pekko.persistence.typed.scaladsl.{Effect, ReplyEffect}
import org.apache.pekko.serialization.SerializerWithStringManifest
import io.cafienne.bounded.aggregate.{AggregateType, DomainCommand, DomainEvent, HandlingFailure, Ko, Ok, Reply}
import io.cafienne.bounded.aggregate.typed.{AggregateBehavior, TypedAggregateRootManager}

import java.io.{
  ByteArrayInputStream,
  ByteArrayOutputStream,
  DataInputStream,
  DataOutputStream,
  NotSerializableException
}

/**
  * A typed aggregate for the cluster specs. It answers commands with Bounded's `Reply`, so `Ok` and `Ko` cross nodes,
  * and tells which node it runs on.
  */
object Notes {
  val Type: AggregateType = AggregateType("notes")

  /** Everything the aggregate sends or stores; bound to the NotesSerializer. */
  sealed trait NotesMessage

  sealed trait NotesCommand extends DomainCommand with NotesMessage

  final case class AddNote(aggregateRootId: String, note: String, replyTo: ActorRef[Reply]) extends NotesCommand
  final case class GetNotes(aggregateRootId: String, replyTo: ActorRef[NoteList])           extends NotesCommand

  /** The notes of an aggregate, and the address of the node it runs on. */
  final case class NoteList(notes: List[String], node: String) extends NotesMessage

  final case class NoteAdded(id: String, note: String) extends DomainEvent with NotesMessage

  final case class NoteRefused(reason: String) extends HandlingFailure with NotesMessage

  class NotesManager extends TypedAggregateRootManager[NotesCommand] {
    override def entityTypeKey: EntityTypeKey[NotesCommand] = EntityTypeKey[NotesCommand](Type.name)

    override def behavior(persistenceId: PersistenceId): Behavior[NotesCommand] =
      Behaviors.setup { context =>
        val clustered = context.system.settings.config.getString("pekko.actor.provider") == "cluster"
        val node      = if (clustered) Cluster(context.system).selfMember.address.toString else "local"
        AggregateBehavior.withEnforcedReplies[NotesCommand, NoteAdded, List[String]](
          persistenceId,
          List.empty,
          (notes, command) => handle(node, notes, command),
          (notes, event) => notes :+ event.note
        )
      }

    private def handle(node: String, notes: List[String], command: NotesCommand): ReplyEffect[NoteAdded, List[String]] =
      command match {
        case AddNote(_, "", replyTo)    => Effect.reply(replyTo)(Ko(NoteRefused("a note cannot be empty")))
        case AddNote(id, note, replyTo) =>
          val event = NoteAdded(id, note)
          Effect.persist(event).thenReply(replyTo)(_ => Ok(List(event)))
        case GetNotes(_, replyTo) => Effect.reply(replyTo)(NoteList(notes, node))
      }
  }
}

/** Serializes the Notes messages without Java serialization, the way an application serializes its own types. */
class NotesSerializer(system: ExtendedActorSystem) extends SerializerWithStringManifest {
  import Notes._

  private lazy val resolver = ActorRefResolver(system.toTyped)

  override def identifier: Int = 1_160_101

  override def manifest(o: AnyRef): String = o.getClass.getSimpleName

  override def toBinary(o: AnyRef): Array[Byte] = {
    val bytes = new ByteArrayOutputStream()
    val out   = new DataOutputStream(bytes)
    o match {
      case AddNote(id, note, replyTo) => write(out, id, note, resolver.toSerializationFormat(replyTo))
      case GetNotes(id, replyTo)      => write(out, id, resolver.toSerializationFormat(replyTo))
      case NoteList(notes, node)      => write(out, (node +: notes)*)
      case NoteAdded(id, note)        => write(out, id, note)
      case NoteRefused(reason)        => write(out, reason)
      case other => throw new IllegalArgumentException(s"Cannot serialize ${other.getClass.getName}")
    }
    out.flush()
    bytes.toByteArray
  }

  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = {
    val fields = read(bytes)
    manifest match {
      case "AddNote"     => AddNote(fields(0), fields(1), resolver.resolveActorRef[Reply](fields(2)))
      case "GetNotes"    => GetNotes(fields(0), resolver.resolveActorRef[NoteList](fields(1)))
      case "NoteList"    => NoteList(fields.tail.toList, fields.head)
      case "NoteAdded"   => NoteAdded(fields(0), fields(1))
      case "NoteRefused" => NoteRefused(fields(0))
      case other         => throw new NotSerializableException(s"Unknown manifest $other")
    }
  }

  private def write(out: DataOutputStream, fields: String*): Unit = {
    out.writeInt(fields.size)
    fields.foreach(out.writeUTF)
  }

  private def read(bytes: Array[Byte]): Vector[String] = {
    val in = new DataInputStream(new ByteArrayInputStream(bytes))
    Vector.fill(in.readInt())(in.readUTF())
  }
}
