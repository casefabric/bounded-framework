/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.serialization

import org.apache.pekko.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.apache.pekko.actor.typed.{ActorRef, Behavior}
import org.apache.pekko.persistence.testkit.PersistenceTestKitPlugin
import org.apache.pekko.persistence.testkit.scaladsl.PersistenceTestKit
import org.apache.pekko.persistence.typed.PersistenceId
import org.apache.pekko.persistence.typed.scaladsl.{Effect, EventSourcedBehavior}
import org.apache.pekko.serialization.{SerializationExtension, Serializers}
import com.typesafe.config.ConfigFactory
import io.cafienne.bounded.aggregate.{CommandMetaData, DomainEvent, EventMetaData, HasEventMetaData}
import org.scalatest.wordspec.AnyWordSpecLike
import spray.json._

import java.io.NotSerializableException
import java.nio.charset.StandardCharsets

object VersionedJsonSerializerSpec {

  /** Version 3: a price in cents and a currency. v1 had `price` in euros, v2 added `currency`, v3 has cents. */
  final case class PriceSet(id: String, cents: Long, currency: String) extends DomainEvent

  final case class Renamed(id: String, name: String, metaData: EventMetaData) extends DomainEvent with HasEventMetaData

  val priceSet: EventFormat[PriceSet] =
    EventFormat("PriceSet", 3, classOf[PriceSet])(
      e => JsObject("id" -> JsString(e.id), "cents" -> JsNumber(e.cents), "currency" -> JsString(e.currency)),
      json =>
        json.asJsObject.getFields("id", "cents", "currency") match {
          case Seq(JsString(id), JsNumber(cents), JsString(currency)) => PriceSet(id, cents.toLongExact, currency)
          case other => throw DeserializationException(s"Not a PriceSet: $other")
        }
    ).upcast(1)(json => JsObject(json.asJsObject.fields + ("currency" -> JsString("EUR"))))
      .upcast(2) { json =>
        val fields = json.asJsObject.fields
        val euros  = fields("price").asInstanceOf[JsNumber].value
        JsObject(fields - "price" + ("cents" -> JsNumber((euros * 100).toLongExact)))
      }

  val renamed: EventFormat[Renamed] =
    EventFormat.withMetaData("Renamed", 1, classOf[Renamed])(
      e => JsObject("id" -> JsString(e.id), "name" -> JsString(e.name)),
      (json, meta) =>
        json.asJsObject.getFields("id", "name") match {
          case Seq(JsString(id), JsString(name)) => Renamed(id, name, meta)
          case other                             => throw DeserializationException(s"Not a Renamed: $other")
        }
    )

  class TestSerializer extends VersionedJsonSerializer {
    override val identifier: Int                        = 1_160_201
    override val formats: Seq[EventFormat[? <: AnyRef]] = List(priceSet, renamed)
  }

  val config = ConfigFactory
    .parseString("""
      pekko.actor {
        serializers.versioned = "io.cafienne.bounded.serialization.VersionedJsonSerializerSpec$TestSerializer"
        serialization-bindings {
          "io.cafienne.bounded.serialization.VersionedJsonSerializerSpec$PriceSet" = versioned
          "io.cafienne.bounded.serialization.VersionedJsonSerializerSpec$Renamed" = versioned
        }
      }
    """)
    .withFallback(PersistenceTestKitPlugin.config)

  final case class SetPrice(cents: Long, replyTo: ActorRef[Long])

  /** Stores prices and answers with the sum, so a recovery shows the events were read back. */
  def prices(persistenceId: String): Behavior[SetPrice] =
    EventSourcedBehavior[SetPrice, PriceSet, Long](
      PersistenceId.ofUniqueId(persistenceId),
      0L,
      (_, command) => Effect.persist(PriceSet(persistenceId, command.cents, "EUR")).thenReply(command.replyTo)(s => s),
      (sum, event) => sum + event.cents
    )
}

class VersionedJsonSerializerSpec
    extends ScalaTestWithActorTestKit(VersionedJsonSerializerSpec.config)
    with AnyWordSpecLike {
  import VersionedJsonSerializerSpec._

  private val serializer = new TestSerializer

  private def json(text: String): Array[Byte] = text.getBytes(StandardCharsets.UTF_8)

  "A versioned JSON serializer" should {
    "write an event as JSON with the manifest <name>.v<version>" in {
      val event = PriceSet("p1", 1250, "EUR")
      serializer.manifest(event) shouldBe "PriceSet.v3"
      new String(serializer.toBinary(event), StandardCharsets.UTF_8).parseJson shouldBe
        JsObject("id" -> JsString("p1"), "cents" -> JsNumber(1250), "currency" -> JsString("EUR"))
      serializer.fromBinary(serializer.toBinary(event), "PriceSet.v3") shouldBe event
    }

    "read older versions through the upcasters, oldest first" in {
      serializer.fromBinary(json("""{"id":"p1","price":12.5}"""), "PriceSet.v1") shouldBe PriceSet("p1", 1250, "EUR")
      serializer.fromBinary(json("""{"id":"p1","price":3,"currency":"USD"}"""), "PriceSet.v2") shouldBe
        PriceSet("p1", 300, "USD")
    }

    "store the metadata next to the event's JSON and hand it back" in {
      val meta   = EventMetaData.causedBy(CommandMetaData.create("request-1", "user-42", Map("tenant" -> "acme")))
      val event  = Renamed("r1", "new name", meta)
      val bytes  = serializer.toBinary(event)
      val fields = new String(bytes, StandardCharsets.UTF_8).parseJson.asJsObject.fields
      fields("data") shouldBe JsObject("id" -> JsString("r1"), "name" -> JsString("new name"))
      fields("meta").asJsObject.fields("correlationId") shouldBe JsString("request-1")
      serializer.fromBinary(bytes, "Renamed.v1") shouldBe event
    }

    "refuse unknown events, newer versions and broken manifests or JSON with a NotSerializableException" in {
      the[NotSerializableException] thrownBy serializer.fromBinary(json("{}"), "Unknown.v1") should have message
        s"Unknown event [Unknown.v1] for ${classOf[TestSerializer].getName}"
      an[NotSerializableException] should be thrownBy serializer.fromBinary(json("{}"), "PriceSet.v4")
      an[NotSerializableException] should be thrownBy serializer.fromBinary(json("{}"), "PriceSet")
      an[NotSerializableException] should be thrownBy serializer.fromBinary(json("{}"), "PriceSet.v0")
      val broken = the[NotSerializableException] thrownBy serializer.fromBinary(json("""{"id":1}"""), "PriceSet.v3")
      broken.getMessage should startWith("Cannot read event [PriceSet.v3]")
      broken.getCause shouldBe a[DeserializationException]
    }

    "list the manifests of every version" in {
      serializer.manifests shouldBe List("PriceSet.v1", "PriceSet.v2", "PriceSet.v3", "Renamed.v1")
    }

    "refuse formats without an upcaster for every older version, and ids reserved by Pekko" in {
      val missing = EventFormat("PriceSet", 3, classOf[PriceSet])(_ => JsNull, _ => PriceSet("", 0, ""))
        .upcast(1)(identity)
      missing.missingUpcasters shouldBe List(2)
      val withGap = new VersionedJsonSerializer {
        override val identifier: Int                        = 1_160_202
        override val formats: Seq[EventFormat[? <: AnyRef]] = List(missing)
      }
      the[IllegalArgumentException] thrownBy withGap.manifests should have message
        "requirement failed: PriceSet v3 has no upcaster from version 2"
      val reserved = new VersionedJsonSerializer {
        override val identifier: Int                        = 17
        override val formats: Seq[EventFormat[? <: AnyRef]] = List(priceSet)
      }
      an[IllegalArgumentException] should be thrownBy reserved.manifest(PriceSet("p", 1, "EUR"))
      an[IllegalArgumentException] should be thrownBy EventFormat("Bad.v1", 1, classOf[PriceSet])(
        _ => JsNull,
        _ => null
      )
    }

    "be the serializer Pekko uses for its events, also in the journal" in {
      val event = PriceSet("p1", 1, "EUR")
      val found = SerializationExtension(system).findSerializerFor(event)
      found shouldBe a[TestSerializer]
      Serializers.manifestFor(found, event) shouldBe "PriceSet.v3"

      val probe = createTestProbe[Long]()
      val first = spawn(prices("prices-1"))
      first ! SetPrice(100, probe.ref)
      first ! SetPrice(250, probe.ref)
      probe.receiveMessages(2)
      testKit.stop(first)
      PersistenceTestKit(system).persistedInStorage("prices-1") shouldBe
        List(PriceSet("prices-1", 100, "EUR"), PriceSet("prices-1", 250, "EUR"))

      val recovered = spawn(prices("prices-1"))
      recovered ! SetPrice(1, probe.ref)
      probe.expectMessage(351L)
    }
  }
}
