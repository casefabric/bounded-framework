/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.test

import io.cafienne.bounded.aggregate.DomainEvent
import io.cafienne.bounded.serialization.{EventFormat, VersionedJsonSerializer}
import org.scalatest.exceptions.TestFailedException
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import spray.json._

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

object EventFixturesSpec {
  final case class Tagged(id: String, tag: String, by: String) extends DomainEvent

  /** v1 had no `by`. */
  class TagSerializer(writeBy: Boolean = true) extends VersionedJsonSerializer {
    override val identifier: Int                        = 1_160_301
    override val formats: Seq[EventFormat[? <: AnyRef]] = List(
      EventFormat("Tagged", 2, classOf[Tagged])(
        e =>
          JsObject(
            Map("id" -> JsString(e.id), "tag" -> JsString(e.tag)) ++
              (if (writeBy) Map("by" -> JsString(e.by)) else Map.empty)
          ),
        json =>
          json.asJsObject.getFields("id", "tag", "by") match {
            case Seq(JsString(id), JsString(tag), JsString(by)) => Tagged(id, tag, by)
            case other                                          => throw DeserializationException(s"Not Tagged: $other")
          }
      ).upcast(1)(json => JsObject(json.asJsObject.fields + ("by" -> JsString("unknown"))))
    )
  }
}

class EventFixturesSpec extends AnyWordSpec with Matchers with EventFixtureChecks {
  import EventFixturesSpec._

  private def folderWith(fixtures: (String, String)*): Path = {
    val folder = Files.createTempDirectory("event-fixtures")
    fixtures.foreach { case (name, json) => Files.write(folder.resolve(name), json.getBytes(StandardCharsets.UTF_8)) }
    folder
  }

  "Event fixtures" should {
    "pass when every version has a fixture that the serializer reads, and writes back for the current version" in {
      val folder = folderWith("Tagged.v1.json" -> """{"id":"a","tag":"red"}""")
      EventFixtures.write(new TagSerializer, folder, List(Tagged("a", "red", "user-1"))) shouldBe
        List(folder.resolve("Tagged.v2.json"))
      EventFixtures.write(new TagSerializer, folder, List(Tagged("b", "blue", "user-2"))) shouldBe empty

      val report = EventFixtures.verify(new TagSerializer, folder)
      report.isOk shouldBe true
      report.checked shouldBe List("Tagged.v1", "Tagged.v2")
      assertEventFixtures(new TagSerializer, folder)
    }

    "report a missing version, a fixture it cannot read, one it writes differently and an unknown one" in {
      val folder = folderWith(
        "Tagged.v1.json" -> """{"id":"a"}""",
        "Gone.v1.json"   -> """{}"""
      )
      val unreadable = EventFixtures.verify(new TagSerializer, folder)
      unreadable.failures.map(_.manifest) shouldBe List("Tagged.v1")
      unreadable.missing shouldBe List("Tagged.v2")
      unreadable.unknown.map(_.getFileName.toString) shouldBe List("Gone.v1.json")

      val current = folderWith(
        "Tagged.v1.json" -> """{"id":"a","tag":"red"}""",
        "Tagged.v2.json" -> """{"id":"a","tag":"red","by":"u"}"""
      )
      val changed = EventFixtures.verify(new TagSerializer(writeBy = false), current)
      changed.failures.map(_.manifest) shouldBe List("Tagged.v2")
      changed.failures.head.reason should include("written back as")

      val failure = intercept[TestFailedException](assertEventFixtures(new TagSerializer, folder))
      failure.getMessage should include("no fixture for Tagged.v2")
    }
  }
}
