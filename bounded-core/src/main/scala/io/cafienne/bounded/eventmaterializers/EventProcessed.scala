/*
 * Copyright (C) 2016-2023 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.eventmaterializers

import java.util.UUID
import akka.persistence.query.Offset

case class EventProcessed(materializerId: UUID, offset: Offset, persistenceId: String, sequenceNr: Long, evt: Any)
