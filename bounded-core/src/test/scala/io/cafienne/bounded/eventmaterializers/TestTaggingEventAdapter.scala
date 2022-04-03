/*
 * Copyright (C) 2016-2022 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.eventmaterializers

import akka.persistence.journal.{Tagged, WriteEventAdapter}
import io.cafienne.bounded.aggregate.DomainEvent

class TestTaggingEventAdapter extends WriteEventAdapter {
  override def manifest(event: Any): String = ""

  override def toJournal(event: Any): Any = event match {
    case prEvent: DomainEvent =>
      Tagged(prEvent, Set("testar"))
    case other => other
  }
}
