/*
 * Copyright (C) 2016-2018 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.akka.persistence.persisters

import io.cafienne.bounded.aggregate._
import com.typesafe.scalalogging.LazyLogging
import spray.json.{DefaultJsonProtocol, RootJsonFormat}
import stamina._

object UnsupportedEventProtocol extends DefaultJsonProtocol {
  import CommandEventDatastructureJsonProtocol._

  case class UnsupportedEventAggregateId(id: String) extends AggregateRootId {
    override def idAsString: String = id
  }
  case class UnsupportedEvent(metaData: MetaData,
                              id: UnsupportedEventAggregateId)
      extends DomainEvent

  implicit val unsupportedAggregateIdFmt
    : RootJsonFormat[UnsupportedEventAggregateId] = jsonFormat1(
    UnsupportedEventAggregateId)
  implicit val unsupportedEventFmt: RootJsonFormat[UnsupportedEvent] =
    jsonFormat2(UnsupportedEvent)
}

object ApPersisters {
  import UnsupportedEventProtocol._
  import stamina.json._

  val unsupportedEventPersister: JsonPersister[UnsupportedEvent, V1] =
    persister[UnsupportedEvent]("UnsupportedEvent")
}

class ForwardsCompatibleSerializer(
    persisters: List[Persister[_, _]],
    codec: PersistedCodec = DefaultPersistedCodec)
    extends StaminaAkkaSerializer(persisters, codec)
    with LazyLogging {

  override def fromBinary(bytes: Array[Byte],
                          clazz: Option[Class[_]]): AnyRef = {
    try {
      super.fromBinary(bytes, clazz)
    } catch {
      case ude: UnsupportedDataException =>
        // If event is derived from aggregatedroot, we assume it is a future event format. The UnsupportedEvent will be
        // returned and will not stop the reader/writer from functioning.
        val persisted = codec.readPersisted(bytes)
        if (persisted.key == "UnsupportedEvent") throw ude

        logger.warn("Unsupported event, converting to UnsupportedEvent event",
                    ude)
        // This will stop the persister (reader/writer) completely in case of failure.
        // In case of failure we either have rubbish, or an event which does not originate from DomainEvent
        ApPersisters.unsupportedEventPersister.unpersist(
          persisted.copy(key = "UnsupportedEvent", version = 1))
    }
  }

}
