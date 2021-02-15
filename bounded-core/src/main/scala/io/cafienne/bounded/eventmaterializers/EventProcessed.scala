/*
 * Copyright (C) 2016-2021 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.eventmaterializers

import java.util.UUID
import akka.persistence.query.Offset

case class EventProcessed(materializerId: UUID, offset: Offset, persistenceId: String, sequenceNr: Long, evt: Any)
