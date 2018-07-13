/*
 * Copyright (C) 2016-2018 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.eventmaterializers

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.persistence.query.{EventEnvelope, Offset}
import akka.stream.scaladsl.Source
import io.cafienne.bounded.{BuildInfo, Compatibility, DefaultCompatibility, RuntimeInfo}

import scala.concurrent.Future

abstract class AbstractReplayableEventMaterializer(
  actorSystem: ActorSystem,
  withPartialReplay: Boolean = true,
  compatible: Compatibility = DefaultCompatibility
)(implicit buildInfo: BuildInfo, runtimeInfo: RuntimeInfo) extends AbstractEventMaterializer(actorSystem, withPartialReplay, compatible)
    with ResumableReplayable {

  import EventMaterializerExecutionContext._

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
      getOffset(viewIdentifier) flatMap { targetOffset =>
        doReplay(targetOffset)
      }
    } else {
      doReplay(Offset.noOffset)
    }
  }

  private def doReplay(targetOffset: Offset) = {
    logger.info(s"Start replay existing events for $viewIdentifier")

    var eventsReplayed = 0
    val source: Source[EventEnvelope, NotUsed] =
      journal.currentEventsByTag(tagName, targetOffset).filter(eventEnvelope => eventFilter(eventEnvelope))
    source
      .runFoldAsync(targetOffset) {
        case (previousOffset, EventEnvelope(offset, persistenceId, sequenceNo, evt)) =>
          logger.debug(
            s"$matMappingName: Received event: $evt(previousOffset: $previousOffset, offset: $offset, persistenceId: $persistenceId, sequenceNo: $sequenceNo)"
          )
          handleReplayEvent(evt) map { _ =>
            logger.debug(s"$matMappingName: Completed processing of event: $evt")
            eventsReplayed += 1
            if (eventsReplayed % 100 == 0) {
              logger.info(s"$viewIdentifier: [$tagName] events replayed: $eventsReplayed")
            }
            if (withPartialReplay) {
              saveOffset(viewIdentifier, offset)
            }
            offset
          }
      }
  }
}
