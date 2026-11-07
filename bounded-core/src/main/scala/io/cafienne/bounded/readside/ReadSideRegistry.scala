/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.readside

import org.apache.pekko.actor.{
  Actor,
  ActorSystem,
  Cancellable,
  ClassicActorSystemProvider,
  ExtendedActorSystem,
  Extension,
  ExtensionId,
  ExtensionIdProvider,
  Props
}
import io.cafienne.bounded.aggregate.EventPosition

import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}

/**
  * How a wait for a read side ended. These are the expected outcomes, so they are values; a failed future means a
  * programming or infrastructure error (an unknown read side, publishing switched off, a terminating actor system).
  *
  * None of them means that the command failed: its events are stored. The caller must not retry or compensate the
  * command on `Pending` or `Failed`.
  */
sealed trait ReadSideResult

/** The read side has processed the position: it shows the command's events. */
case object Processed extends ReadSideResult

/** The read side has not processed the position yet, but may still do so. */
final case class Pending(reason: Pending.Reason) extends ReadSideResult

object Pending {
  sealed trait Reason

  /** The wait timed out. */
  case object TimedOut extends Reason

  /** The read side's stream stopped with this failure while the caller waited; it may restart and catch up. */
  final case class Down(cause: Throwable) extends Reason
}

/**
  * The read side gave up on an event at or before the position (`ProjectionFailure` with `isFinal`): it will not
  * show the command's events without intervention.
  */
final case class Failed(cause: Throwable) extends ReadSideResult

/**
  * Waits until a read side has processed a position, to answer a command only once its read model shows it ("read
  * your own write"). It listens to the `ProjectionProgress`, `ProjectionFailure` and `ProjectionDown` that read sides
  * publish when `bounded.eventmaterializers.publish` is on, and knows per read side and persistence id the highest
  * sequence number processed since it started.
  *
  * {{{
  * gateway.ask(id, replyTo => PlaceOrder(id, replyTo)).flatMap {
  *   case Persisted(_, position) => ReadSideRegistry(system).awaitProcessed(orderOverview.readSideId, position)
  *   case Ok(_)                  => Future.successful(Processed) // no events: nothing to wait for
  *   case Ko(failure)            => ...
  * }
  * }}}
  *
  * A read side registers itself when it is built (a projection) or starts listening (an event materializer); waiting
  * for one that never registered fails. The registry is node-local: it hears the read sides that run on this node.
  *
  * Since it only knows what it saw since it started, a wait for an event processed before a restart, or before its
  * entry was dropped (`bounded.read-side.idle-retention`), ends in `Pending(TimedOut)`. So do not wait when a command
  * stored no events, and keep the idle retention far above the lag of the read side.
  */
final class ReadSideRegistry private (system: ExtendedActorSystem) extends Extension {

  private val config = system.settings.config

  /** Whether the read sides publish the signals the registry needs. */
  val isPublishing: Boolean =
    config.hasPath("bounded.eventmaterializers.publish") && config.getBoolean("bounded.eventmaterializers.publish")

  /** The timeout of `awaitProcessed` when none is given, `bounded.read-side.timeout`. */
  val defaultTimeout: FiniteDuration = duration("bounded.read-side.timeout")

  /** How long a read side's entry for a persistence id is kept without waiters, `bounded.read-side.idle-retention`. */
  val idleRetention: FiniteDuration = duration("bounded.read-side.idle-retention")

  private implicit val executionContext: ExecutionContext = system.dispatcher

  private final class Waiter(val sequenceNr: Long, val promise: Promise[ReadSideResult]) {
    var timer: Option[Cancellable] = None

    def complete(result: ReadSideResult): Unit = {
      timer.foreach(_.cancel())
      promise.trySuccess(result)
      ()
    }
  }

  private final class Entry {
    var processed: Long                            = 0L
    var failure: Option[(Long, Throwable)]         = None
    var waiters: List[Waiter]                      = Nil
    var touched: Long                              = System.nanoTime()
    def touch(): Unit                              = touched = System.nanoTime()
    def idleSince(now: Long, limit: Long): Boolean = waiters.isEmpty && now - touched > limit
  }

  // All state is guarded by this lock; the work under it is small and never blocks.
  private val lock                  = new Object
  private val registered            = mutable.Set[String]()
  private val entries               = mutable.Map[(String, String), Entry]()
  @volatile private var terminating = false

  /**
    * Registers a read side, so it can be waited for. The read sides of Bounded register themselves; call this for a
    * read side of your own that publishes the signals.
    */
  def register(readSideId: String): Unit = lock.synchronized {
    registered += readSideId
    ()
  }

  /** Whether the read side is registered. */
  def isRegistered(readSideId: String): Boolean = lock.synchronized(registered.contains(readSideId))

  /**
    * Completes once the read side has processed the position, gave up on an event at or before it, stopped, or the
    * timeout passed. It completes at once when the read side already processed the position (or gave up on it), so
    * waiting after the reply of the command has no race.
    *
    * Fails with `IllegalArgumentException` for a read side that is not registered, and with `IllegalStateException`
    * when publishing is off or the actor system terminates.
    */
  def awaitProcessed(
    readSideId: String,
    position: EventPosition,
    timeout: FiniteDuration = defaultTimeout
  ): Future[ReadSideResult] = {
    if (terminating) Future.failed(new IllegalStateException("The actor system is terminating"))
    else if (!isPublishing)
      Future.failed(
        new IllegalStateException(
          "The read sides do not publish their progress: set bounded.eventmaterializers.publish = on"
        )
      )
    else
      lock.synchronized {
        if (!registered.contains(readSideId))
          Future.failed(
            new IllegalArgumentException(
              s"Read side [$readSideId] is not registered; registered are: ${registered.toList.sorted.mkString(", ")}"
            )
          )
        else {
          val entry = entries.getOrElseUpdate((readSideId, position.persistenceId), new Entry)
          entry.touch()
          entry.failure match {
            case _ if entry.processed >= position.sequenceNr                => Future.successful(Processed)
            case Some((failedAt, cause)) if failedAt <= position.sequenceNr => Future.successful(Failed(cause))
            case _                                                          =>
              val waiter = new Waiter(position.sequenceNr, Promise[ReadSideResult]())
              entry.waiters = waiter :: entry.waiters
              waiter.timer = Some(system.scheduler.scheduleOnce(timeout)(timedOut(entry, waiter)))
              waiter.promise.future
          }
        }
      }
  }

  private def timedOut(entry: Entry, waiter: Waiter): Unit = {
    lock.synchronized {
      entry.waiters = entry.waiters.filterNot(_ eq waiter)
      entry.touch()
    }
    waiter.complete(Pending(Pending.TimedOut))
  }

  private[readside] def signal(signal: ReadSideSignal): Unit = {
    val completions: List[(Waiter, ReadSideResult)] = lock.synchronized {
      signal match {
        case ProjectionProgress(id, persistenceId, sequenceNr) =>
          val entry = entries.getOrElseUpdate((id, persistenceId), new Entry)
          entry.touch()
          entry.processed = entry.processed.max(sequenceNr)
          // The event the read side gave up on is processed after all, e.g. after a restart
          if (entry.failure.exists { case (failedAt, _) => failedAt <= sequenceNr }) entry.failure = None
          val (done, waiting) = entry.waiters.partition(_.sequenceNr <= entry.processed)
          entry.waiters = waiting
          done.map(_ -> Processed)

        case ProjectionFailure(id, persistenceId, sequenceNr, cause, true) =>
          val entry = entries.getOrElseUpdate((id, persistenceId), new Entry)
          entry.touch()
          entry.failure = Some(sequenceNr -> cause)
          val (failed, waiting) = entry.waiters.partition(_.sequenceNr >= sequenceNr)
          entry.waiters = waiting
          failed.map(_ -> Failed(cause))

        case _: ProjectionFailure => Nil

        case ProjectionDown(id, cause) =>
          entries
            .collect { case ((`id`, _), entry) =>
              val down = entry.waiters
              entry.waiters = Nil
              entry.touch()
              down.map(_ -> Pending(Pending.Down(cause)))
            }
            .flatten
            .toList
      }
    }
    completions.foreach { case (waiter, result) => waiter.complete(result) }
  }

  private def dropIdleEntries(): Unit = lock.synchronized {
    val now   = System.nanoTime()
    val limit = idleRetention.toNanos
    entries.filterInPlace { case (_, entry) => !entry.idleSince(now, limit) }
    ()
  }

  private def terminated(): Unit = {
    terminating = true
    val waiting = lock.synchronized {
      val all = entries.values.flatMap(_.waiters).toList
      entries.clear()
      all
    }
    waiting.foreach { waiter =>
      waiter.timer.foreach(_.cancel())
      waiter.promise.tryFailure(new IllegalStateException("The actor system terminated"))
    }
  }

  private def duration(path: String): FiniteDuration =
    FiniteDuration(config.getDuration(path).toNanos, TimeUnit.NANOSECONDS)

  private val listener = system.systemActorOf(Props(new ReadSideRegistry.Listener(this)), "bounded-read-side-registry")
  system.eventStream.subscribe(listener, classOf[ReadSideSignal])

  private val cleanup: Cancellable = {
    val interval = (idleRetention / 2).max(1.second)
    system.scheduler.scheduleWithFixedDelay(interval, interval)(() => dropIdleEntries())
  }

  system.registerOnTermination {
    cleanup.cancel()
    terminated()
  }
}

object ReadSideRegistry extends ExtensionId[ReadSideRegistry] with ExtensionIdProvider {

  override def lookup: ReadSideRegistry.type = ReadSideRegistry

  override def createExtension(system: ExtendedActorSystem): ReadSideRegistry = new ReadSideRegistry(system)

  override def get(system: ActorSystem): ReadSideRegistry = super.get(system)

  override def get(system: ClassicActorSystemProvider): ReadSideRegistry = super.get(system)

  private final class Listener(registry: ReadSideRegistry) extends Actor {
    override def receive: Receive = { case signal: ReadSideSignal =>
      registry.signal(signal)
    }
  }
}
