/*
 * Copyright (C) 2016-2018 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.akka.persistence.eventmaterializers

import akka.persistence.query.{Offset}
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.util.{Failure, Success}

class EventMaterializers(replayables: List[Resumable]) {

  import EventMaterializerExecutionContext._
  import EventMaterializers._

  lazy val logger: Logger = Logger(LoggerFactory.getLogger("bounded.eventmaterializers"))

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
        replayable
          .replayEvents()
          .map(replayOffset => startListing(ReplayResult(replayable, Some(replayOffset)), keepListenersRunning))
      case nonReplayable: Resumable =>
        Future(startListing(ReplayResult(nonReplayable, None), keepListenersRunning))
    })
  }

  private def startListing(replayed: ReplayResult, keepRunning: Boolean): ReplayResult = {
    if (keepRunning) {
      replayed.viewMaterializer
        .registerListener(replayed.offset)
        .onComplete({
          case Success(msg) =>
            logger.info("Listener {} is done msg: {}", replayed.viewMaterializer, msg)
          case Failure(msg) =>
            logger.error("Listener {} stopped with a failure: {}", replayed.viewMaterializer, msg)
        })
    }
    replayed
  }
}

object EventMaterializers {

  case class ReplayResult(viewMaterializer: Resumable, offset: Option[Offset])
}
