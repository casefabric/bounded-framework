/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.eventmaterializers

import org.apache.pekko.Done
import org.apache.pekko.persistence.query.Offset

import scala.concurrent.Future

trait Resumable {
  def registerListener(maybeStartOffset: Option[Offset]): Future[Done]
}

trait ResumableReplayable extends Resumable {
  def replayEvents(): Future[Offset]
}
