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

import akka.persistence.query.Offset
import com.typesafe.scalalogging.{Logger}
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.util.{Failure, Success}

class EventMaterializers(replayables: List[ResumableReplayable],
                         nonReplayables: List[Resumable]) {

  import EventMaterializerExecutionContext._

  lazy val logger: Logger = Logger(
    LoggerFactory.getLogger("bounded.eventmaterializers"))

  case class ReplayResult(resumableReplayable: ResumableReplayable,
                          offset: Offset)

  /**
    * Start event listeners in given order: First do a replay and *after* all replays have
    * finished start listening for new events (register listener)
    *
    * @param callbackFunction function that is called when replaying has finished.
    */
  def startUp(callbackFunction: () => Unit): Unit = {

    val fSerialized = replayables.foldLeft(
      Future.successful[List[ReplayResult]](Nil)) { (f, replayable) =>
      f.flatMap { x =>
        replayable.replayEvents().map(ReplayResult(replayable, _) :: x)
      }
    } map (_.reverse)

    fSerialized onComplete {
      case Success(result) =>
        logger.info("Replays finished, Register listeners")
        result.foreach(
          r =>
            handleListenerResult(
              r.resumableReplayable.registerListener(Some(r.offset))))
        nonReplayables.foreach(nr =>
          handleListenerResult(nr.registerListener(Option.empty)))
        logger.info("Done registering listeners")
        callbackFunction()
      case Failure(t) =>
        logger.error("Error during replay", t)
        throw new IllegalStateException("Replay failed", t)
    }

  }

  private def handleListenerResult(listener: Future[Any]): Unit = {
    listener onComplete {
      case Success(result) =>
        logger.info("Listener stopped")
      case Failure(t) =>
        logger.error("Error during replay", t)
        throw new IllegalStateException("Listener aborted with error", t)
    }
  }
}
