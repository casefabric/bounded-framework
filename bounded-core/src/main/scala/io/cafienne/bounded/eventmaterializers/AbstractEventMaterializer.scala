/*
 * Copyright (C) 2016-2022 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.eventmaterializers

import java.util.UUID

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.persistence.query.{EventEnvelope, Offset}
import akka.stream.{KillSwitches, UniqueKillSwitch}
import akka.stream.scaladsl.{Keep, Sink, Source}
import io.cafienne.bounded.akka.ActorSystemProvider
import io.cafienne.bounded.akka.persistence.ReadJournalProvider
import io.cafienne.bounded.config.Configured
import com.typesafe.scalalogging.Logger
import io.cafienne.bounded.aggregate.DomainEvent
import io.cafienne.bounded.eventmaterializers.offsetstores.OffsetStore

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
  * @param compatible indicate what events this materializer should process.
  *                   @see io.cafienne.bounded.Compatibility for more info
  */
abstract class AbstractEventMaterializer(
  actorSystem: ActorSystem,
  keepCurrentOffset: Boolean = true,
  materialzerEventFilter: MaterializerEventFilter = NoFilterEventFilter
) extends ActorSystemProvider
    with ReadJournalProvider
    with OffsetStore
    with Resumable
    with ExceptionWriter
    with Configured {

  /**
    * Unique ID for this materializer
    */
  val materializerId = UUID.randomUUID()

  override implicit def system: ActorSystem = actorSystem

  val logger: Logger

  implicit val mat = actorSystem.dispatcher

  val journal = readJournal

  /**
    * Tagname used to identify eventstream to listen to
    */
  val tagName: String

  /**
    * Mapping name of this listener
    */
  val matMappingName: String

  def viewIdentifier: String = matMappingName

  /**
    * Handle new incoming event
    *
    * @param evt event
    */
  def handleEvent(evt: Any): Future[Done]

  /**
    * Publish an event on the AKKA eventbus after the event is processed.
    */
  val isPublishRequired: Boolean = (actorSystem.settings.config
    .hasPath("bounded.eventmaterializers.publish") && actorSystem.settings.config
    .getBoolean("bounded.eventmaterializers.publish"))

  /**
    * Register listener for events. Should be registered *after* replay is finished
    * @return Future
    */
  override def registerListener(maybeStartOffset: Option[Offset]): Future[Done] = {
    val (_, offsetFuture) = registerListenerWithKillSwitch(maybeStartOffset)
    offsetFuture.mapTo[Done]
  }

  def registerListenerWithKillSwitch(maybeStartOffset: Option[Offset]): (UniqueKillSwitch, Future[Offset]) = {
    logger.info(s"Registering listener with killswitch $viewIdentifier")

    val listenStartOffset = maybeStartOffset.getOrElse(Await.result(getOffset(viewIdentifier), 10.seconds))
    val source: Source[EventEnvelope, NotUsed] =
      journal
        .eventsByTag(tagName, listenStartOffset)
        .filter(eventEnvelope => materialzerEventFilter.filter(eventEnvelope.event.asInstanceOf[DomainEvent]))
    val lastSnk = Sink.last[Offset]
    val answer = source
      .mapAsync(1) {
        case EventEnvelope(evtOffset, persistenceId, sequenceNo, evt) =>
          logger.debug(
            s"$matMappingName: runStream: Received event: $evt(offset: $evtOffset, persistenceId: $persistenceId, sequenceNo: $sequenceNo)"
          )
          handleEvent(evt) map { _ =>
            if (isPublishRequired) {
              system.eventStream.publish(EventProcessed(materializerId, evtOffset, persistenceId, sequenceNo, evt))
            }

            if (keepCurrentOffset) {
              saveOffset(viewIdentifier, evtOffset)
            }
            logger.debug(s"$matMappingName: runStream: Completed processing of event: $evt")
            evtOffset
          }
      }
      .viaMat(KillSwitches.single)(Keep.right)
      .toMat(lastSnk)(Keep.both)

    answer.run()
  }
}
