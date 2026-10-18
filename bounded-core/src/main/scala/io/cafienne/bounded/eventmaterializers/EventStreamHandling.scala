/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.eventmaterializers

import java.util.UUID
import org.apache.pekko.{Done, NotUsed}
import org.apache.pekko.persistence.query.{EventEnvelope, Offset}
import org.apache.pekko.stream.{KillSwitches, UniqueKillSwitch}
import org.apache.pekko.stream.scaladsl.{Keep, Sink, Source}
import com.typesafe.scalalogging.Logger
import io.cafienne.bounded.aggregate.DomainEvent
import io.cafienne.bounded.pekko.ActorSystemProvider
import io.cafienne.bounded.eventmaterializers.offsetstores.OffsetStore

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

/**
  * The event stream steps shared by the event materializers, for replay as well as live listening:
  * filtering the events, handling an event, publishing that it was processed and saving its offset.
  */
private[eventmaterializers] trait EventStreamHandling extends ActorSystemProvider with OffsetStore {

  def logger: Logger

  def matMappingName: String

  def viewIdentifier: String

  def materializerId: UUID

  def isPublishRequired: Boolean

  def handleEvent(evt: Any): Future[Done]

  private def executionContext: ExecutionContext = system.dispatcher

  /**
    * The events accepted by the filter.
    */
  protected def filtered(
    events: Source[EventEnvelope, NotUsed],
    filter: MaterializerEventFilter
  ): Source[EventEnvelope, NotUsed] =
    events.filter(eventEnvelope => filter.filter(eventEnvelope.event.asInstanceOf[DomainEvent]))

  /**
    * The offset to start listening from: the given one, otherwise the stored one.
    */
  protected def listenStartOffset(maybeStartOffset: Option[Offset]): Offset =
    maybeStartOffset.getOrElse(Await.result(getOffset(viewIdentifier), 10.seconds))

  /**
    * Handles one event, then publishes an EventProcessed when `publish` is set and saves the offset when
    * `keepOffset` is set.
    *
    * @return the offset of the event, once the handler is done
    */
  protected def processEvent(
    eventEnvelope: EventEnvelope,
    handler: Any => Future[Done],
    publish: Boolean,
    keepOffset: Boolean
  ): Future[Offset] = {
    val evtOffset     = eventEnvelope.offset
    val persistenceId = eventEnvelope.persistenceId
    val sequenceNo    = eventEnvelope.sequenceNr
    val evt           = eventEnvelope.event
    logger.debug(
      s"$matMappingName: Received event: $evt(offset: $evtOffset, persistenceId: $persistenceId, sequenceNo: $sequenceNo)"
    )
    handler(evt).map { _ =>
      if (publish) {
        system.eventStream.publish(EventProcessed(materializerId, evtOffset, persistenceId, sequenceNo, evt))
      }
      if (keepOffset) {
        saveOffset(viewIdentifier, evtOffset)
      }
      logger.debug(s"$matMappingName: Completed processing of event: $evt")
      evtOffset
    }(executionContext)
  }

  /**
    * Handles the live events one by one until the stream is stopped with the kill switch.
    *
    * @return the kill switch, and the offset of the last handled event once the stream has stopped
    */
  protected def runListener(
    events: Source[EventEnvelope, NotUsed],
    keepOffset: Boolean
  ): (UniqueKillSwitch, Future[Offset]) =
    events
      .mapAsync(1)(processEvent(_, handleEvent, isPublishRequired, keepOffset))
      .viaMat(KillSwitches.single)(Keep.right)
      .toMat(Sink.last[Offset])(Keep.both)
      .run()
}
