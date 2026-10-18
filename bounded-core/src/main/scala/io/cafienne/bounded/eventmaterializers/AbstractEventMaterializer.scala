/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.eventmaterializers

import java.util.UUID
import org.apache.pekko.Done
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.persistence.query.Offset
import org.apache.pekko.stream.UniqueKillSwitch
import io.cafienne.bounded.pekko.persistence.ReadJournalProvider
import io.cafienne.bounded.config.Configured
import com.typesafe.scalalogging.Logger

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
abstract class AbstractEventMaterializer(
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
    logger.info(s"Registering listener with killswitch $viewIdentifier")

    runListener(
      offset => journal.eventsByTag(tagName, offset).map(PositionedEvent(_)),
      listenStartOffset(maybeStartOffset),
      keepCurrentOffset,
      materialzerEventFilter
    )
  }
}
