/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.serialization

import org.apache.pekko.serialization.SerializerWithStringManifest
import io.cafienne.bounded.aggregate.{EventMetaData, HasEventMetaData}
import spray.json._

import java.io.NotSerializableException
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.util.UUID
import scala.util.control.NonFatal

/**
  * How one event class is stored: its name, its current version, how its current version is written to and read
  * from JSON, and an upcaster for every older version, which turns the JSON of version `n` into that of `n + 1`.
  * Build it with `EventFormat(...)` or `EventFormat.withMetaData(...)`, and add the upcasters with `upcast`.
  */
final class EventFormat[T <: AnyRef] private (
  val name: String,
  val version: Int,
  val eventClass: Class[T],
  private[serialization] val write: T => (JsValue, Option[EventMetaData]),
  private[serialization] val read: (JsValue, Option[EventMetaData]) => T,
  private[serialization] val upcasters: Map[Int, JsValue => JsValue],
  val carriesMetaData: Boolean
) {
  require(name.nonEmpty && !name.contains(".v"), s"An event name is non-empty and has no '.v': [$name]")
  require(version >= 1, s"The version of $name must be 1 or more, not $version")

  /** The manifest of the current version: `<name>.v<version>`. */
  def manifest: String = EventFormat.manifest(name, version)

  /** The manifests of all versions, oldest first. */
  def manifests: Seq[String] = (1 to version).map(EventFormat.manifest(name, _))

  /** Adds the upcaster from version `from` to `from + 1`. */
  def upcast(from: Int)(upcaster: JsValue => JsValue): EventFormat[T] = {
    require(from >= 1 && from < version, s"$name has no version $from to upcast from (current: $version)")
    require(!upcasters.contains(from), s"$name already has an upcaster from version $from")
    new EventFormat(name, version, eventClass, write, read, upcasters + (from -> upcaster), carriesMetaData)
  }

  /** The versions without an upcaster to the next. */
  def missingUpcasters: Seq[Int] = (1 until version).filterNot(upcasters.contains)
}

object EventFormat {

  /** The format of an event without metadata, or whose codec writes its metadata itself. */
  def apply[T <: AnyRef](name: String, version: Int, eventClass: Class[T])(
    write: T => JsValue,
    read: JsValue => T
  ): EventFormat[T] =
    new EventFormat[T](name, version, eventClass, e => (write(e), None), (json, _) => read(json), Map.empty, false)

  /**
    * The format of an event that carries `EventMetaData`: the serializer stores the metadata next to the JSON of
    * `write` (`{"meta": …, "data": …}`) and hands it back to `read`, so the codec does not have to know it.
    * Upcasters see the `data` part only.
    */
  def withMetaData[T <: AnyRef with HasEventMetaData](name: String, version: Int, eventClass: Class[T])(
    write: T => JsValue,
    read: (JsValue, EventMetaData) => T
  ): EventFormat[T] =
    new EventFormat[T](
      name,
      version,
      eventClass,
      e => (write(e), Some(e.metaData)),
      (json, meta) => read(json, meta.getOrElse(throw new NotSerializableException(s"Event $name has no metadata"))),
      Map.empty,
      true
    )

  private[serialization] def manifest(name: String, version: Int): String = s"$name.v$version"
}

/**
  * Base of a Pekko serializer for events, stored as UTF-8 JSON with the manifest `<EventName>.v<n>`; never the class
  * name, so a class can be renamed or moved. An event stored with an older version is read through the upcasters of
  * its format, oldest first, up to the current version.
  *
  * {{{
  * class OrderSerializer(system: ExtendedActorSystem) extends VersionedJsonSerializer {
  *   override val identifier: Int = 4_200_001
  *   override val formats = List(
  *     EventFormat("OrderPlaced", 2, classOf[OrderPlaced])(placedFormat.write, placedFormat.read)
  *       .upcast(1)(json => JsObject(json.asJsObject.fields + ("currency" -> JsString("EUR"))))
  *   )
  * }
  * }}}
  *
  * Bind it in `pekko.actor.serializers` and `serialization-bindings` to the event classes. Pekko refuses a second
  * serializer with the same `identifier` when it starts; ids 0 to 40 are Pekko's own and refused here.
  *
  * A stored event must always be readable: one event that cannot be read stops the recovery of its aggregate and
  * every read side that reads it. Keep a fixture of every version of every event and check them with
  * `bounded-test`'s `EventFixtures`. An unknown manifest, a version newer than the current one, a missing upcaster or
  * a failing codec throw a `NotSerializableException` that names the manifest.
  */
abstract class VersionedJsonSerializer extends SerializerWithStringManifest {

  /** The formats of the events this serializer stores, one per event class and per name. */
  def formats: Seq[EventFormat[? <: AnyRef]]

  private lazy val checked: Seq[EventFormat[? <: AnyRef]] = {
    require(identifier > 40, s"Serializer id $identifier of ${getClass.getName} is in Pekko's reserved range 0 to 40")
    val names = formats.map(_.name)
    require(
      names.distinct.size == names.size,
      s"Event names are not unique: ${names.diff(names.distinct).mkString(", ")}"
    )
    val classes = formats.map(_.eventClass)
    require(classes.distinct.size == classes.size, s"An event class has more than one format in ${getClass.getName}")
    formats.foreach { format =>
      require(
        format.missingUpcasters.isEmpty,
        s"${format.name} v${format.version} has no upcaster from version ${format.missingUpcasters.mkString(", ")}"
      )
    }
    formats
  }

  private lazy val byClass: Map[Class[?], EventFormat[? <: AnyRef]] = checked.map(f => f.eventClass -> f).toMap
  private lazy val byName: Map[String, EventFormat[? <: AnyRef]]    = checked.map(f => f.name -> f).toMap

  /** The manifests of all versions of all events, for fixture checks. */
  def manifests: Seq[String] = checked.flatMap(_.manifests)

  private def formatOf(o: AnyRef): EventFormat[AnyRef] =
    byClass
      .getOrElse(
        o.getClass,
        throw new IllegalArgumentException(s"${getClass.getName} has no format for ${o.getClass.getName}")
      )
      .asInstanceOf[EventFormat[AnyRef]]

  override def manifest(o: AnyRef): String = formatOf(o).manifest

  override def toBinary(o: AnyRef): Array[Byte] = {
    val format        = formatOf(o)
    val (data, meta)  = format.write(o)
    val json: JsValue =
      if (format.carriesMetaData) JsObject("meta" -> VersionedJsonSerializer.writeMeta(meta), "data" -> data)
      else data
    json.compactPrint.getBytes(StandardCharsets.UTF_8)
  }

  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef = {
    val (name, version) = VersionedJsonSerializer.parseManifest(manifest)
    val format          = byName
      .getOrElse(name, throw new NotSerializableException(s"Unknown event [$manifest] for ${getClass.getName}"))
      .asInstanceOf[EventFormat[AnyRef]]
    if (version > format.version)
      throw new NotSerializableException(s"Event [$manifest] is newer than ${format.name} v${format.version}")
    try {
      val json         = new String(bytes, StandardCharsets.UTF_8).parseJson
      val (data, meta) =
        if (format.carriesMetaData) {
          val fields = json.asJsObject.fields
          (fields("data"), Some(VersionedJsonSerializer.readMeta(fields("meta"))))
        } else (json, None)
      val upcast = (version until format.version).foldLeft(data)((current, from) => format.upcasters(from)(current))
      format.read(upcast, meta)
    } catch {
      case e: NotSerializableException => throw e
      case NonFatal(e)                 =>
        val failure = new NotSerializableException(s"Cannot read event [$manifest]: ${e.getMessage}")
        failure.initCause(e)
        throw failure
    }
  }
}

object VersionedJsonSerializer {

  /** `<name>.v<n>` as name and version. */
  def parseManifest(manifest: String): (String, Int) = {
    val at      = manifest.lastIndexOf(".v")
    val version =
      if (at > 0) manifest.substring(at + 2).toIntOption.filter(_ >= 1)
      else None
    version match {
      case Some(v) => (manifest.substring(0, at), v)
      case None    => throw new NotSerializableException(s"Manifest [$manifest] is not <EventName>.v<n>")
    }
  }

  private[serialization] def writeMeta(meta: Option[EventMetaData]): JsValue = meta match {
    case None    => JsNull
    case Some(m) =>
      JsObject(
        "eventId"       -> JsString(m.eventId.toString),
        "correlationId" -> JsString(m.correlationId),
        "causationId"   -> m.causationId.fold[JsValue](JsNull)(JsString(_)),
        "actor"         -> JsString(m.actor),
        "timestamp"     -> JsString(m.timestamp.toString),
        "fields"        -> JsObject(m.fields.map { case (k, v) => k -> JsString(v) }),
        "migrated"      -> JsBoolean(m.migrated)
      )
  }

  private[serialization] def readMeta(json: JsValue): EventMetaData = {
    val fields                      = json.asJsObject.fields
    def string(key: String): String = fields.get(key) match {
      case Some(JsString(value)) => value
      case other                 => throw new DeserializationException(s"Metadata field $key is $other")
    }
    EventMetaData(
      UUID.fromString(string("eventId")),
      string("correlationId"),
      fields.get("causationId").collect { case JsString(value) => value },
      string("actor"),
      Instant.parse(string("timestamp")),
      fields.get("fields").map(_.asJsObject.fields.collect { case (k, JsString(v)) => k -> v }).getOrElse(Map.empty),
      fields.get("migrated").contains(JsBoolean(true))
    )
  }
}
