/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.aggregate

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.{Clock, Instant, ZoneOffset}

object MetaDataSpec {
  final case class PlaceOrder(aggregateRootId: String, metaData: CommandMetaData)
      extends DomainCommand
      with HasCommandMetaData
  final case class OrderPlaced(id: String, metaData: EventMetaData) extends DomainEvent with HasEventMetaData
  final case class Created(id: String)                              extends DomainEvent
}

class MetaDataSpec extends AnyWordSpec with Matchers {
  import MetaDataSpec._

  private val clock = Clock.fixed(Instant.parse("2026-11-07T10:00:00Z"), ZoneOffset.UTC)

  "Command and event metadata" should {
    "start a chain for a request from outside" in {
      val command = CommandMetaData.create("request-1", "user-42", Map("tenant" -> "acme"), clock)
      command.correlationId shouldBe "request-1"
      command.causationId shouldBe None
      command.actor shouldBe "user-42"
      command.timestamp shouldBe clock.instant()
      command.fields shouldBe Map("tenant" -> "acme")
    }

    "pass correlation, actor and fields from a command to its events, with the command as cause" in {
      val command = CommandMetaData.create("request-1", "user-42", Map("tenant" -> "acme"), clock)
      val event   = EventMetaData.causedBy(command, clock)
      event.correlationId shouldBe "request-1"
      event.causationId shouldBe Some(command.commandId.toString)
      event.actor shouldBe "user-42"
      event.fields shouldBe Map("tenant" -> "acme")
      event.migrated shouldBe false
      event.eventId should not be command.commandId
    }

    "pass them from an event to the command a policy sends, with the event as cause" in {
      val event   = EventMetaData.causedBy(CommandMetaData.create("request-1", "user-42", Map("tenant" -> "acme")))
      val command = CommandMetaData.causedBy(event, actor = Some("system:policy"), clock = clock)
      command.correlationId shouldBe "request-1"
      command.causationId shouldBe Some(event.eventId.toString)
      command.actor shouldBe "system:policy"
      command.fields shouldBe Map("tenant" -> "acme")
      CommandMetaData.causedBy(event).actor shouldBe "user-42"
    }

    "mark imported events as migrated" in {
      val imported = EventMetaData.imported("import-2026-11", "system:import", clock = clock)
      imported.migrated shouldBe true
      imported.causationId shouldBe None
    }

    "be found on commands and events that carry it" in {
      val command = CommandMetaData.create("request-1", "user-42")
      val event   = EventMetaData.causedBy(command)
      CommandMetaData.of(PlaceOrder("o1", command)) shouldBe Some(command)
      EventMetaData.of(OrderPlaced("o1", event)) shouldBe Some(event)
      CommandMetaData.of("not a command") shouldBe None
      EventMetaData.of(Created("no metadata")) shouldBe None
    }
  }
}
