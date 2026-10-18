/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.eventmaterializers

import java.util.UUID
import org.apache.pekko.persistence.query.Offset

case class EventProcessed(materializerId: UUID, offset: Offset, persistenceId: String, sequenceNr: Long, evt: Any)
