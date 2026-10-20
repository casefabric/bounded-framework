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
import org.apache.pekko.stream.scaladsl.{Flow, Keep, RestartSource, Sink, Source}
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
  * A failing event stream is restarted with backoff and resumes after the last event that was handled. The
  * saved offset can lag behind that (see `savePositions`). Events are therefore handled at least once: after a
  * restart a handler can see the event it failed on again, and after a new start the events after the saved
  * offset, so handlers must be idempotent.
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
    * Whether the filter accepts the event. The filter only judges domain events; any other event is accepted.
    */
  protected def accepts(filter: MaterializerEventFilter, evt: Any): Boolean = evt match {
    case domainEvent: DomainEvent => filter.filter(domainEvent)
    case _                        => true
  }

  /**
    * The offset to start listening from: the given one, otherwise the stored one.
    */
  protected def listenStartOffset(maybeStartOffset: Option[Offset]): Offset =
    maybeStartOffset.getOrElse(Await.result(getOffset(viewIdentifier), 10.seconds))

  /**
    * Handles one event and publishes an EventProcessed when `publish` is set. A failing handler fails the
    * returned future. An event the filter rejects is not handled or published, but its position is still
    * returned, so that `savePositions` can save it and a stretch of filtered events is not read again.
    *
    * @return the position after the event, once it is handled
    */
  protected def processEvent(
    event: PositionedEvent,
    filter: MaterializerEventFilter,
    handler: Any => Future[Done],
    publish: Boolean
  ): Future[Offset] = {
    val eventEnvelope = event.envelope
    val evtOffset     = eventEnvelope.offset
    val persistenceId = eventEnvelope.persistenceId
    val sequenceNo    = eventEnvelope.sequenceNr
    val evt           = eventEnvelope.event
    val accepted      = accepts(filter, evt)
    logger.debug(
      s"$matMappingName: Received event: $evt(offset: $evtOffset, persistenceId: $persistenceId, sequenceNo: $sequenceNo, filtered out: ${!accepted})"
    )
    (if (accepted) handler(evt) else Future.successful(Done)).map { _ =>
      if (publish && accepted) {
        system.eventStream.publish(EventProcessed(materializerId, evtOffset, persistenceId, sequenceNo, evt))
      }
      logger.debug(s"$matMappingName: Completed processing of event: $evt")
      event.position
    }
  }

  /**
    * Saves the positions of handled events when `keepOffset` is set, one save at a time. While a save runs, the
    * positions handled meanwhile are collapsed into the latest one, so handling does not wait for the offset store
    * and a run of filtered events costs about one save. A position is saved only after its event is handled; on a
    * stop, positions not saved yet are handled again after the next start (handlers are at-least-once anyway).
    * A failing save fails the stream.
    *
    * @return the positions as they are saved
    */
  protected def savePositions(keepOffset: Boolean): Flow[Offset, Offset, NotUsed] =
    if (!keepOffset) Flow[Offset]
    else
      Flow[Offset]
        .conflate((_, latest) => latest)
        .mapAsync(1)(position => saveOffset(viewIdentifier, position).map(_ => position))

  /**
    * Handles the live events one by one until the stream is stopped with the kill switch. After a failure
    * the stream restarts with backoff from the position after the last event handled in this JVM.
    *
    * @param events the live events after a given position
    * @param startOffset the position to start from
    * @param filter the events to handle
    * @return the kill switch, and the position after the last handled event (the start position when none
    *         was) once the stream has stopped. The future fails when the restarts are used up.
    */
  protected def runListener(
    events: Offset => Source[PositionedEvent, NotUsed],
    startOffset: Offset,
    keepOffset: Boolean,
    filter: MaterializerEventFilter
  ): (UniqueKillSwitch, Future[Offset]) = {
    val resumeFrom = new AtomicReference[Offset](startOffset)
    RestartSource
      .onFailuresWithBackoff(restartSettings) { () =>
        logger.debug(s"$viewIdentifier: listening for events after offset ${resumeFrom.get}")
        events(resumeFrom.get)
          // resumeFrom is set as soon as the event is handled, also when a failing save tears the stream down meanwhile
          .mapAsync(1)(processEvent(_, filter, handleEvent, isPublishRequired).map { offset =>
            resumeFrom.set(offset)
            offset
          })
          .via(savePositions(keepOffset))
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
    * Retries a failing attempt with the backoff and the number of restarts of the restart settings. Settings
    * that RestartSource accepts but RetrySupport does not are mapped: a negative max-restarts retries without a
    * limit, like it restarts without one, and the backoff and random factor are brought into RetrySupport's range.
    */
  protected def retryWithBackoff[T](attempt: () => Future[T]): Future[T] = {
    val retry = EventStreamHandling.RetrySettings(restartSettings)
    RetrySupport.retry(
      () => {
        val result = attempt()
        result.failed.foreach(e => logger.warn(s"$viewIdentifier: event stream failed", e))
        result
      },
      retry.attempts,
      retry.minBackoff,
      retry.maxBackoff,
      retry.randomFactor
    )(executionContext, system.scheduler)
  }
}

/**
  * An event and the position of the materializer once the event is processed. For a materializer that reads
  * one tag the position is the event's offset; for a saga it is the offset per tag.
  */
private[eventmaterializers] final case class PositionedEvent(envelope: EventEnvelope, position: Offset)

private[eventmaterializers] object PositionedEvent {
  def apply(envelope: EventEnvelope): PositionedEvent = PositionedEvent(envelope, envelope.offset)
}

private[eventmaterializers] object EventStreamHandling {

  def restartSettings(config: Config): RestartSettings =
    RestartSettings(
      duration(config, "min-backoff"),
      duration(config, "max-backoff"),
      config.getDouble("random-factor")
    ).withMaxRestarts(config.getInt("max-restarts"), duration(config, "max-restarts-within"))

  final case class RetrySettings(attempts: Int, minBackoff: FiniteDuration, maxBackoff: FiniteDuration, randomFactor: Double)

  object RetrySettings {
    // RetrySupport counts the retries after the first attempt, which matches maxRestarts
    def apply(settings: RestartSettings): RetrySettings = {
      val minBackoff = settings.minBackoff.max(1.millisecond)
      RetrySettings(
        if (settings.maxRestarts < 0) Int.MaxValue else settings.maxRestarts,
        minBackoff,
        settings.maxBackoff.max(minBackoff),
        settings.randomFactor.max(0.0).min(1.0)
      )
    }
  }

  private def duration(config: Config, path: String): FiniteDuration =
    FiniteDuration(config.getDuration(path).toNanos, TimeUnit.NANOSECONDS)
}
