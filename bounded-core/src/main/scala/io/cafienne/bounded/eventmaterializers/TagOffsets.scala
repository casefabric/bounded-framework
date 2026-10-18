/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.eventmaterializers

import org.apache.pekko.persistence.query.Offset

/**
  * The position per tag of an event materializer that reads several tags, stored as one offset record.
  * It is only stored and read by Bounded: each tag's own offset is what is handed to the read journal.
  *
  * @param offsets offset per tag name
  */
final case class TagOffsets(offsets: Map[String, Offset]) extends Offset
