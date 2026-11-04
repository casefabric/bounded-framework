/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.eventmaterializers

import org.apache.pekko.Done
import org.apache.pekko.persistence.query.Offset
import org.apache.pekko.stream.UniqueKillSwitch

import scala.concurrent.Future

trait Resumable {
  def registerListener(maybeStartOffset: Option[Offset]): Future[Done]
}

trait ResumableReplayable extends Resumable {
  def replayEvents(): Future[Offset]
}

/**
  * A materializer whose listening can be stopped, which `ClusteredEventMaterializers` needs to move it to another node.
  * `AbstractEventMaterializer` (and so `AbstractReplayableEventMaterializer`) and `CombinedLogicEventMaterializer`
  * implement it.
  */
trait StoppableResumable extends Resumable {

  /** Identifies the materializer and its stored offset; unique among the materializers that run together. */
  def viewIdentifier: String

  /**
    * Starts listening, like `registerListener`.
    *
    * @return the kill switch that stops listening, and the position after the last handled event once listening has
    *         stopped. The future fails when the restarts of the event stream are used up.
    */
  def registerListenerWithKillSwitch(maybeStartOffset: Option[Offset]): (UniqueKillSwitch, Future[Offset])
}
