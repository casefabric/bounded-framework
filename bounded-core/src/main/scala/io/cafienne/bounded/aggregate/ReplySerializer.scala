/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.aggregate

import org.apache.pekko.actor.ExtendedActorSystem
import org.apache.pekko.serialization.{SerializationExtension, SerializerWithStringManifest, Serializers}

import java.io.{
  ByteArrayInputStream,
  ByteArrayOutputStream,
  DataInputStream,
  DataOutputStream,
  NotSerializableException
}
import scala.collection.immutable.Seq

/**
  * Serializes the replies of aggregates (`Ok`, `Persisted`, `Ko`) and Bounded's own handling failures, so they can cross nodes
  * in a cluster without Java serialization. It is bound to them in Bounded's `reference.conf`.
  *
  * The events in an `Ok`, the failure in a `Ko` and the command in an `UnexpectedCommand` are the application's own
  * types: they are written with the serializer Pekko has bound to them, which the application must configure (the
  * events need one for the journal anyway). The cause of a `PersistenceFailed` or `PersistenceRejected` keeps only
  * its class name and message: it is read back as a `RemoteFailureCause`.
  */
class ReplySerializer(system: ExtendedActorSystem) extends SerializerWithStringManifest {
  import ReplySerializer._

  // Looked up on first use: the extension is not ready while serializers are being created.
  private lazy val serialization = SerializationExtension(system)

  override def identifier: Int = Identifier

  override def manifest(o: AnyRef): String = o match {
    case _: Ok                  => OkManifest
    case _: Persisted           => PersistedManifest
    case _: Ko                  => KoManifest
    case _: PersistenceFailed   => PersistenceFailedManifest
    case _: PersistenceRejected => PersistenceRejectedManifest
    case _: UnexpectedCommand   => UnexpectedCommandManifest
    case other                  => throw new IllegalArgumentException(s"Cannot serialize ${other.getClass.getName}")
  }

  override def toBinary(o: AnyRef): Array[Byte] = {
    val bytes = new ByteArrayOutputStream()
    val out   = new DataOutputStream(bytes)
    o match {
      case Ok(events) =>
        out.writeInt(events.size)
        events.foreach(writePayload(out, _))
      case Persisted(events, position) =>
        out.writeInt(events.size)
        events.foreach(writePayload(out, _))
        out.writeUTF(position.persistenceId)
        out.writeLong(position.sequenceNr)
      case Ko(failure)                   => writePayload(out, failure)
      case PersistenceFailed(cause)      => writeCause(out, cause)
      case PersistenceRejected(cause)    => writeCause(out, cause)
      case unexpected: UnexpectedCommand => writePayload(out, unexpected.command)
      case other => throw new IllegalArgumentException(s"Cannot serialize ${other.getClass.getName}")
    }
    out.flush()
    bytes.toByteArray
  }

  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = {
    val in = new DataInputStream(new ByteArrayInputStream(bytes))
    manifest match {
      case OkManifest        => Ok(Seq.fill(in.readInt())(readPayload(in).asInstanceOf[DomainEvent]))
      case PersistedManifest =>
        val events = Seq.fill(in.readInt())(readPayload(in).asInstanceOf[DomainEvent])
        Persisted(events, EventPosition(in.readUTF(), in.readLong()))
      case KoManifest                  => Ko(readPayload(in).asInstanceOf[HandlingFailure])
      case PersistenceFailedManifest   => PersistenceFailed(readCause(in))
      case PersistenceRejectedManifest => PersistenceRejected(readCause(in))
      case UnexpectedCommandManifest   => new UnexpectedCommand(readPayload(in).asInstanceOf[DomainCommand])
      case other                       => throw new NotSerializableException(s"Unknown manifest $other")
    }
  }

  private def writePayload(out: DataOutputStream, payload: AnyRef): Unit = {
    val serializer = serialization.findSerializerFor(payload)
    val bytes      = serializer.toBinary(payload)
    out.writeInt(serializer.identifier)
    out.writeUTF(Serializers.manifestFor(serializer, payload))
    out.writeInt(bytes.length)
    out.write(bytes)
  }

  private def readPayload(in: DataInputStream): AnyRef = {
    val serializerId = in.readInt()
    val manifest     = in.readUTF()
    val bytes        = new Array[Byte](in.readInt())
    in.readFully(bytes)
    serialization.deserialize(bytes, serializerId, manifest).get
  }

  private def writeCause(out: DataOutputStream, cause: Throwable): Unit = cause match {
    case remote: RemoteFailureCause =>
      out.writeUTF(remote.className)
      out.writeUTF(remote.originalMessage)
    case _ =>
      out.writeUTF(cause.getClass.getName)
      out.writeUTF(Option(cause.getMessage).getOrElse(""))
  }

  private def readCause(in: DataInputStream): Throwable = new RemoteFailureCause(in.readUTF(), in.readUTF())
}

object ReplySerializer {
  val Identifier: Int = 1_160_001

  private val OkManifest                  = "Ok"
  private val PersistedManifest           = "Persisted"
  private val KoManifest                  = "Ko"
  private val PersistenceFailedManifest   = "PersistenceFailed"
  private val PersistenceRejectedManifest = "PersistenceRejected"
  private val UnexpectedCommandManifest   = "UnexpectedCommand"
}

/**
  * The cause of a `PersistenceFailed` or `PersistenceRejected` that was sent from another node: the class name and
  * message of the original exception.
  */
final class RemoteFailureCause(val className: String, val originalMessage: String)
    extends RuntimeException(s"$className: $originalMessage")
