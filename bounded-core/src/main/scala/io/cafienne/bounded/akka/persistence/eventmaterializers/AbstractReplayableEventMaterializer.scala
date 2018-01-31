// Copyright (C) 2018 the original author or authors.
// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package io.cafienne.bounded.akka.persistence.eventmaterializers

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.persistence.query.{EventEnvelope, Offset}
import akka.stream.scaladsl.Source

import scala.concurrent.Future

abstract class AbstractReplayableEventMaterializer(actorSystem: ActorSystem,
                                                   withPartialReplay: Boolean =
                                                     true)
    extends AbstractEventMaterializer(actorSystem, withPartialReplay)
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
      doReplay(startOffset)
    }
  }

  private def doReplay(targetOffset: Offset) = {
    logger.info(s"Start replay existing events for $viewIdentifier")

    var eventsReplayed = 0
    val source: Source[EventEnvelope, NotUsed] =
      journal.currentEventsByTag(tagName, targetOffset)
    source
      .runFoldAsync(targetOffset) {
        case (previousOffset,
              EventEnvelope(offset, persistenceId, sequenceNo, evt)) =>
          logger.debug(
            s"$matMappingName: Received event: $evt(previousOffset: $previousOffset, offset: $offset, persistenceId: $persistenceId, sequenceNo: $sequenceNo)")
          handleReplayEvent(evt) map { _ =>
            logger.debug(
              s"$matMappingName: Completed processing of event: $evt")
            eventsReplayed += 1
            if (eventsReplayed % 100 == 0) {
              logger.info(
                s"$viewIdentifier: [$tagName] events replayed: $eventsReplayed")
              if (withPartialReplay) {
                saveOffset(viewIdentifier, offset)
              }
            }
            offset
          }
      }
  }
}
