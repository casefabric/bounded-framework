/*
 * Copyright (C) 2016-2018 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.akka.persistence.eventmaterializers

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.persistence.query.{EventEnvelope, Offset}
import akka.stream.{ActorMaterializer, KillSwitches, UniqueKillSwitch}
import akka.stream.scaladsl.{Keep, Sink, Source}
import io.cafienne.bounded.akka.ActorSystemProvider
import io.cafienne.bounded.akka.persistence.ReadJournalProvider
import io.cafienne.bounded.config.Configured
import com.typesafe.scalalogging.Logger

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

/**
  * Abstract class to be used to create eventlisteners. Use this class as a base for listening for
  * specific events. The class supports offset store to keep track of events received.
  *
  * Set tagname to identify eventstream to listen to
  * Set mappingname as identifier for this listener. It will be used as the id for storing current offset of
  * listener.
  *
  * @param actorSystem
  * @param keepCurrentOffset Set this to false if current offset must not be stored.
  */
abstract class AbstractEventMaterializer(actorSystem: ActorSystem,
                                         keepCurrentOffset: Boolean = true)
    extends ActorSystemProvider
    with ReadJournalProvider
    with OffsetStore
    with Resumable
    with ExceptionWriter
    with Configured {

  import EventMaterializerExecutionContext._

  override implicit def system: ActorSystem = actorSystem

  val logger: Logger

  implicit val mat = ActorMaterializer()

  val journal = readJournal

  /**
    * Tagname used to identify eventstream to listen to
    */
  val tagName: String

  /**
    * Mapping name of this listener
    */
  val matMappingName: String

  val env = if (config.hasPath("pp.env")) config.getString("pp.env") else ""

  def viewIdentifier: String = env + ":" + matMappingName

  /**
    * Handle new incoming event
    *
    * @param evt event
    */
  def handleEvent(evt: Any): Future[Done]

  /**
    * Register listener for events. Should be registered *after* replay is finished
    * @return Future
    */
  override def registerListener(
      maybeStartOffset: Option[Offset]): Future[Done] = {
    val (_, offsetFuture) = registerListenerWithKillSwitch(maybeStartOffset)
    offsetFuture.mapTo[Done]
  }

  def registerListenerWithKillSwitch(
      maybeStartOffset: Option[Offset]): (UniqueKillSwitch, Future[Offset]) = {
    logger.info(s"Registering listener with killswitch $viewIdentifier")

    val listenStartOffset = maybeStartOffset.getOrElse(
      Await.result(getOffset(viewIdentifier), 10.seconds))
    val source: Source[EventEnvelope, NotUsed] =
      journal.eventsByTag(tagName, listenStartOffset)
    val lastSnk = Sink.last[Offset]
    val answer = source
      .mapAsync(1) {
        case EventEnvelope(evtOffset, persistenceId, sequenceNo, evt) =>
          logger.debug(
            s"$matMappingName: runStream: Received event: $evt(offset: $evtOffset, persistenceId: $persistenceId, sequenceNo: $sequenceNo)")
          handleEvent(evt) map { _ =>
            if (keepCurrentOffset) {
              saveOffset(viewIdentifier, evtOffset)
            }
            logger.debug(
              s"$matMappingName: runStream: Completed processing of event: $evt")
            evtOffset
          }
      }
      .viaMat(KillSwitches.single)(Keep.right)
      .toMat(lastSnk)(Keep.both)

    answer.run()
  }
}
