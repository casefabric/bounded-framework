/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.eventmaterializers

import org.apache.pekko.Done
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.persistence.query.Offset

import scala.concurrent.Future

abstract class AbstractReplayableEventMaterializer(
  actorSystem: ActorSystem,
  withPartialReplay: Boolean = true,
  materializerEventFilter: MaterializerEventFilter = NoFilterEventFilter
) extends AbstractEventMaterializer(actorSystem, withPartialReplay, materializerEventFilter)
    with ResumableReplayable {

  /**
    * Handle event during replay
    *
    * @param evt event
    */
  def handleReplayEvent(evt: Any): Future[Done]

  /**
    * Events are replayed from startoffset until the last known handled event
    *
    * @return Future with last replayed offset
    */
  override def replayEvents(): Future[Offset] = {
    if (withPartialReplay) {
      getOffset(viewIdentifier) flatMap { targetOffset => doReplay(targetOffset) }
    } else {
      doReplay(Offset.noOffset)
    }
  }

  private def doReplay(targetOffset: Offset) = {
    logger.info(s"Start replay existing events for $viewIdentifier")

    var eventsReplayed = 0
    filtered(journal.currentEventsByTag(tagName, targetOffset), materializerEventFilter)
      .runFoldAsync(targetOffset) { (_, eventEnvelope) =>
        processEvent(eventEnvelope, handleReplayEvent, publish = false, keepOffset = withPartialReplay) map { offset =>
          eventsReplayed += 1
          if (eventsReplayed % 100 == 0) {
            logger.info(s"$viewIdentifier: [$tagName] events replayed: $eventsReplayed")
          }
          offset
        }
      }
  }
}
