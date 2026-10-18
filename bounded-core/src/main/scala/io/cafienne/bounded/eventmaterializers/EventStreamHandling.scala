/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.eventmaterializers

import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import org.apache.pekko.{Done, NotUsed}
import org.apache.pekko.pattern.RetrySupport
import org.apache.pekko.persistence.query.{EventEnvelope, Offset}
import org.apache.pekko.stream.{KillSwitches, RestartSettings, UniqueKillSwitch}
import org.apache.pekko.stream.scaladsl.{Keep, RestartSource, Sink, Source}
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import io.cafienne.bounded.aggregate.DomainEvent
import io.cafienne.bounded.pekko.ActorSystemProvider
import io.cafienne.bounded.eventmaterializers.offsetstores.OffsetStore

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

/**
  * The event stream steps shared by the event materializers, for replay as well as live listening:
  * filtering the events, handling an event, saving its offset and publishing that it was processed.
  *
  * A failing event stream is restarted with backoff and resumes after the last event that was handled (and,
  * when offsets are kept, saved). Events are therefore handled at least once: after a restart a handler can
  * see the event it failed on again, so handlers must be idempotent.
  */
private[eventmaterializers] trait EventStreamHandling extends ActorSystemProvider with OffsetStore {

  def logger: Logger

  def matMappingName: String

  def viewIdentifier: String

  def materializerId: UUID

  def isPublishRequired: Boolean

  def handleEvent(evt: Any): Future[Done]

  private implicit def executionContext: ExecutionContext = system.dispatcher

  /**
    * How the event streams restart after a failure, by default from `bounded.eventmaterializers.restart`.
    */
  protected def restartSettings: RestartSettings =
    EventStreamHandling.restartSettings(system.settings.config.getConfig("bounded.eventmaterializers.restart"))

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
    * Handles one event, then saves the offset when `keepOffset` is set and publishes an EventProcessed when
    * `publish` is set. The offset is saved only after the handler is done, and a failing handler or offset
    * save fails the returned future.
    *
    * @return the offset of the event, once it is handled and saved
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
    for {
      _ <- handler(evt)
      _ <- if (keepOffset) saveOffset(viewIdentifier, evtOffset) else Future.unit
    } yield {
      if (publish) {
        system.eventStream.publish(EventProcessed(materializerId, evtOffset, persistenceId, sequenceNo, evt))
      }
      logger.debug(s"$matMappingName: Completed processing of event: $evt")
      evtOffset
    }
  }

  /**
    * Handles the live events one by one until the stream is stopped with the kill switch. After a failure
    * the stream restarts with backoff from the last event handled (and saved) in this JVM.
    *
    * @param events the live events after a given offset
    * @param startOffset the offset to start from
    * @return the kill switch, and the offset of the last handled event (the start offset when none was) once
    *         the stream has stopped. The future fails when the restarts are used up.
    */
  protected def runListener(
    events: Offset => Source[EventEnvelope, NotUsed],
    startOffset: Offset,
    keepOffset: Boolean
  ): (UniqueKillSwitch, Future[Offset]) = {
    val resumeFrom = new AtomicReference[Offset](startOffset)
    RestartSource
      .onFailuresWithBackoff(restartSettings) { () =>
        logger.debug(s"$viewIdentifier: listening for events after offset ${resumeFrom.get}")
        events(resumeFrom.get)
          .mapAsync(1)(processEvent(_, handleEvent, isPublishRequired, keepOffset))
          .map { offset =>
            resumeFrom.set(offset)
            offset
          }
          .mapError {
            case e =>
              logger.warn(s"$viewIdentifier: event stream failed after offset ${resumeFrom.get}", e)
              e
          }
      }
      .viaMat(KillSwitches.single)(Keep.right)
      .toMat(Sink.fold(startOffset)((_, offset) => offset))(Keep.both)
      .run()
  }

  /**
    * Retries a failing attempt with the backoff and the number of restarts of the restart settings.
    */
  protected def retryWithBackoff[T](attempt: () => Future[T]): Future[T] = {
    val settings = restartSettings
    // RetrySupport counts the retries after the first attempt, which matches maxRestarts
    RetrySupport.retry(
      () => {
        val result = attempt()
        result.failed.foreach(e => logger.warn(s"$viewIdentifier: event stream failed", e))
        result
      },
      settings.maxRestarts,
      settings.minBackoff,
      settings.maxBackoff,
      settings.randomFactor
    )(executionContext, system.scheduler)
  }
}

private[eventmaterializers] object EventStreamHandling {

  def restartSettings(config: Config): RestartSettings =
    RestartSettings(
      duration(config, "min-backoff"),
      duration(config, "max-backoff"),
      config.getDouble("random-factor")
    ).withMaxRestarts(config.getInt("max-restarts"), duration(config, "max-restarts-within"))

  private def duration(config: Config, path: String): FiniteDuration =
    FiniteDuration(config.getDuration(path).toNanos, TimeUnit.NANOSECONDS)
}
