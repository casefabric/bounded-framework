/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.eventmaterializers

import java.util.concurrent.atomic.AtomicReference
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
    * Handle event during replay. A failed replay is retried from the last replayed event, so an event can be
    * handled more than once: make the handler idempotent.
    *
    * @param evt event
    */
  def handleReplayEvent(evt: Any): Future[Done]

  /**
    * Events are replayed from startoffset until the last known handled event. A failing replay is retried
    * with the backoff of the restart settings, from the last replayed event.
    *
    * @return Future with last replayed offset
    */
  override def replayEvents(): Future[Offset] = {
    val startOffset = if (withPartialReplay) getOffset(viewIdentifier) else Future.successful(Offset.noOffset)
    startOffset flatMap { offset =>
      val replayedUpTo = new AtomicReference[Offset](offset)
      retryWithBackoff(() => doReplay(replayedUpTo))
    }
  }

  private def doReplay(replayedUpTo: AtomicReference[Offset]): Future[Offset] = {
    val targetOffset = replayedUpTo.get
    logger.info(s"Start replay existing events for $viewIdentifier from offset $targetOffset")

    var eventsReplayed = 0
    journal
      .currentEventsByTag(tagName, targetOffset)
      .runFoldAsync(targetOffset) { (_, eventEnvelope) =>
        val processed = processEvent(
          PositionedEvent(eventEnvelope),
          materializerEventFilter,
          handleReplayEvent,
          publish = false,
          keepOffset = withPartialReplay
        )
        processed map { offset =>
          replayedUpTo.set(offset)
          eventsReplayed += 1
          if (eventsReplayed % 100 == 0) {
            logger.info(s"$viewIdentifier: [$tagName] events replayed: $eventsReplayed")
          }
          offset
        }
      }
  }
}
