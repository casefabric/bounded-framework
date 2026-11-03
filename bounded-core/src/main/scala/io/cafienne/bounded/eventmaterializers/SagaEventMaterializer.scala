/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.eventmaterializers

import java.util.UUID
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.persistence.query.{NoOffset, Offset}
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.stream.UniqueKillSwitch
import org.apache.pekko.{Done, NotUsed}
import com.typesafe.scalalogging.Logger
import io.cafienne.bounded.aggregate.AggregateType
import io.cafienne.bounded.pekko.persistence.{AggregateTypeReadJournal, ReadJournalProvider}
import io.cafienne.bounded.config.Configured

import scala.concurrent.{ExecutionContextExecutor, Future}

/**
  * Abstract class to be used to create eventlisteners. Use this class as a base for listening for
  * specific events. The class supports offset store to keep track of events received.
  *
  * Set aggregateTypes to the types of the aggregates whose events to listen to
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

  val journal: AggregateTypeReadJournal = aggregateTypeJournal

  /**
    * The types of the aggregates whose events the saga reads. The saga keeps its offset per type.
    */
  val aggregateTypes: List[AggregateType]

  private def typeNames: List[String] = aggregateTypes.map(_.name)

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
    require(aggregateTypes.nonEmpty, s"Saga $viewIdentifier must listen to at least one aggregate type")
    logger.info(s"Registering saga with killswitch $viewIdentifier")

    val startPosition = SagaEventMaterializer.tagOffsets(typeNames, listenStartOffset(maybeStartOffset))
    runListener(events, startPosition, keepCurrentOffset, materialzerEventFilter)
  }

  /**
    * The events of all aggregate types after the given position, each with the position per type once it is
    * processed.
    */
  private def events(position: Offset): Source[PositionedEvent, NotUsed] = {
    val start = SagaEventMaterializer.tagOffsets(typeNames, position)
    Source(aggregateTypes)
      .flatMapMerge(
        aggregateTypes.size,
        aggregateType =>
          journal.eventsByAggregateType(aggregateType, start.offsets(aggregateType.name)).map(aggregateType.name -> _)
      )
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
    * The position per aggregate type (stored per type name, the tag of its events) of a saga. A stored TagOffsets
    * gives each type its own offset (a type without one starts from the beginning, a type no longer listened to is
    * dropped). A plain offset, stored by an older version of Bounded or given to registerListener, is the start
    * offset of every type.
    */
  private[eventmaterializers] def tagOffsets(tagNames: List[String], position: Offset): TagOffsets =
    position match {
      case TagOffsets(offsets) => TagOffsets(tagNames.map(tag => tag -> offsets.getOrElse(tag, NoOffset)).toMap)
      case plain               => TagOffsets(tagNames.map(tag => tag -> plain).toMap)
    }
}
