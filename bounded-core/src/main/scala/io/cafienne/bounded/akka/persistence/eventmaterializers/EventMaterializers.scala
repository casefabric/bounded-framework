// Copyright (C) 2018 the original author or authors.
// See the LICENSE file distributed with this work for additional
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

import akka.persistence.query.{NoOffset, Offset}
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.util.{Failure, Success}

class EventMaterializers(replayables: List[Resumable]) {

  import EventMaterializerExecutionContext._

  lazy val logger: Logger = Logger(
    LoggerFactory.getLogger("bounded.eventmaterializers"))

  case class ReplayResult(viewMaterializer: Resumable,
                          offset: Offset)

  /**
    * Start event listeners in given order: First do a replay and *after* all replays have
    * finished start listening for new events (register listener)
    *
    * @param keepListenersRunning gives the option to only replay (false) or continue after replay (true)
    * @return The list of Offsets for the replayed (and possibly started) event materializers
    *         When the event materializer is *NOT* replayable, the offset will be NoOffset
    */
  def startUp(keepListenersRunning: Boolean): Future[List[ReplayResult]] = {
    Future.sequence(replayables map {
      case replayable: ResumableReplayable =>
        replayable.replayEvents().map(replayOffset =>
          startListing(ReplayResult(replayable, replayOffset), keepListenersRunning))
      case nonReplayable: Resumable =>
        Future(startListing(ReplayResult(nonReplayable, NoOffset), keepListenersRunning))
    })
  }

  private def startListing(replayed: ReplayResult, keepRunning: Boolean): ReplayResult = {
    if (keepRunning) {
      replayed.viewMaterializer.registerListener(Some(replayed.offset)).onComplete({
        case Success(msg) => logger.info("Listener {} is done msg: {}", replayed.viewMaterializer, msg)
        case Failure(msg) => logger.error("Listener {} stopped with a failure: {}", replayed.viewMaterializer, msg)
      })
    }
    replayed
  }
}
