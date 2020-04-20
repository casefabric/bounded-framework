/*
 * Copyright (C) 2016-2020 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.eventmaterializers

import akka.Done
import akka.persistence.query.Offset

import scala.concurrent.Future

trait Resumable {
  def registerListener(maybeStartOffset: Option[Offset]): Future[Done]
}

trait ResumableReplayable extends Resumable {
  def replayEvents(): Future[Offset]
}
