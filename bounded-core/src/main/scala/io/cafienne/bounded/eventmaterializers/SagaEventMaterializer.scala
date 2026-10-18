/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.eventmaterializers

import java.util.UUID
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.persistence.query.scaladsl.{
  CurrentEventsByPersistenceIdQuery,
  CurrentEventsByTagQuery,
  EventsByTagQuery,
  ReadJournal
}
import org.apache.pekko.persistence.query.{NoOffset, Offset}
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.stream.UniqueKillSwitch
import org.apache.pekko.{Done, NotUsed}
import com.typesafe.scalalogging.Logger
import io.cafienne.bounded.pekko.persistence.ReadJournalProvider
import io.cafienne.bounded.config.Configured

import scala.concurrent.{ExecutionContextExecutor, Future}

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
abstract class SagaEventMaterializer(
  actorSystem: ActorSystem,
  keepCurrentOffset: Boolean = true,
  materialzerEventFilter: MaterializerEventFilter = NoFilterEventFilter
) extends EventStreamHandling
    with ReadJournalProvider
    with Resumable
    with ExceptionWriter
    with Configured {

  /**
    * Unique ID for this materializer
    */
  val materializerId = UUID.randomUUID()

  override implicit def system: ActorSystem = actorSystem

  val logger: Logger

  implicit val mat: ExecutionContextExecutor = actorSystem.dispatcher

  val journal: ReadJournal with CurrentEventsByTagQuery with EventsByTagQuery with CurrentEventsByPersistenceIdQuery =
    readJournal

  /**
    * Tagnames used to identify the eventstreams to listen to. The saga keeps its offset per tag.
    */
  val tagNames: List[String]

  /**
    * Mapping name of this listener
    */
  val matMappingName: String

  def viewIdentifier: String = matMappingName

  /**
    * Handle new incoming event. After a failure the event stream restarts from the last handled event, so an
    * event can be handled more than once: make the handler idempotent.
    *
    * @param evt event
    */
  def handleEvent(evt: Any): Future[Done]

  /**
    * Publish an event on the Pekko eventbus after the event is processed.
    */
  val isPublishRequired: Boolean = (actorSystem.settings.config
    .hasPath("bounded.eventmaterializers.publish") && actorSystem.settings.config
    .getBoolean("bounded.eventmaterializers.publish"))

  /**
    * Register listener for events. Should be registered *after* replay is finished
    * @return Future that completes when the listener is stopped, and fails when its restarts are used up
    */
  override def registerListener(maybeStartOffset: Option[Offset]): Future[Done] = {
    val (_, offsetFuture) = registerListenerWithKillSwitch(maybeStartOffset)
    offsetFuture.map(_ => Done)
  }

  def registerListenerWithKillSwitch(maybeStartOffset: Option[Offset]): (UniqueKillSwitch, Future[Offset]) = {
    require(tagNames.nonEmpty, s"Saga $viewIdentifier must listen to at least one tag")
    logger.info(s"Registering saga with killswitch $viewIdentifier")

    val startPosition = SagaEventMaterializer.tagOffsets(tagNames, listenStartOffset(maybeStartOffset))
    runListener(events, startPosition, keepCurrentOffset, materialzerEventFilter)
  }

  /**
    * The events of all tags after the given position, each with the position per tag once it is processed.
    */
  private def events(position: Offset): Source[PositionedEvent, NotUsed] = {
    val start = SagaEventMaterializer.tagOffsets(tagNames, position)
    Source(tagNames)
      .flatMapMerge(tagNames.size, tag => journal.eventsByTag(tag, start.offsets(tag)).map(tag -> _))
      .statefulMap(() => start)(
        (positions, taggedEnvelope) => {
          val (tag, envelope) = taggedEnvelope
          val next            = TagOffsets(positions.offsets.updated(tag, envelope.offset))
          (next, PositionedEvent(envelope, next))
        },
        _ => None
      )
  }
}

object SagaEventMaterializer {

  /**
    * The position per tag of a saga. A stored TagOffsets gives each tag its own offset (a tag without one
    * starts from the beginning, a tag no longer listened to is dropped). A plain offset, stored by an older
    * version of Bounded or given to registerListener, is the start offset of every tag.
    */
  private[eventmaterializers] def tagOffsets(tagNames: List[String], position: Offset): TagOffsets =
    position match {
      case TagOffsets(offsets) => TagOffsets(tagNames.map(tag => tag -> offsets.getOrElse(tag, NoOffset)).toMap)
      case plain               => TagOffsets(tagNames.map(tag => tag -> plain).toMap)
    }
}
