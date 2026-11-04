/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.eventmaterializers

import org.apache.pekko.actor.typed.{ActorSystem, Behavior}
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors}
import org.apache.pekko.cluster.sharding.typed.ShardedDaemonProcessSettings
import org.apache.pekko.cluster.sharding.typed.scaladsl.ShardedDaemonProcess
import org.apache.pekko.persistence.query.Offset
import org.apache.pekko.stream.UniqueKillSwitch

import scala.util.{Failure, Success, Try}

/**
  * Runs event materializers in a cluster, each on exactly one node at a time, with Pekko's `ShardedDaemonProcess`:
  * the materializers are spread over the nodes, and when a node leaves, its materializers resume on another node
  * from their stored offsets. A materializer that writes to a store shared by the nodes, and every combined logic,
  * must run this way (or on one node only): otherwise every node handles every event and they overwrite each other's
  * offsets. A read model each node keeps for itself (in memory, or with the in-memory or LMDB offset store) runs on
  * every node with `EventMaterializers` instead.
  *
  * Every node of the cluster that runs materializers calls `start` with the same `name` and the same materializers
  * (by `viewIdentifier`; their order does not matter). A replayable materializer replays first and then listens,
  * like in `EventMaterializers`, but each on its own: one materializer does not wait for the replays of the others.
  *
  * Delivery stays at-least-once: when a materializer moves, the events after its last saved offset are handled again
  * on the new node. A materializer that stops with a failure (its restarts used up) is started again by the daemon
  * process within `pekko.cluster.sharded-daemon-process.keep-alive-interval`. Without a split brain resolver both
  * halves of a split cluster run every materializer.
  *
  * @param system a typed actor system with `pekko.actor.provider = cluster`
  * @param materializers the materializers, each with a unique `viewIdentifier`
  * @param name the name of the daemon process, the same on every node
  */
class ClusteredEventMaterializers(
  system: ActorSystem[_],
  materializers: List[StoppableResumable],
  name: String = "bounded-event-materializers"
) {
  import ClusteredEventMaterializers._

  require(materializers.nonEmpty, "There are no event materializers to run")
  require(
    materializers.map(_.viewIdentifier).distinct.size == materializers.size,
    s"The viewIdentifiers of the event materializers are not unique: ${materializers.map(_.viewIdentifier).mkString(", ")}"
  )

  // Every node numbers the materializers the same way, whatever order they are given in.
  private val ordered = materializers.sortBy(_.viewIdentifier).toVector

  /** Starts the daemon process on this node; the materializers start on the nodes they are allocated to. */
  def start(): Unit =
    ShardedDaemonProcess(system).init[Command](
      name,
      ordered.size,
      index => runner(ordered(index)),
      ShardedDaemonProcessSettings(system),
      Some(Stop)
    )
}

object ClusteredEventMaterializers {

  /** The messages of a runner; they never leave the node. */
  sealed trait Command
  private case object Stop                                       extends Command
  private final case class Replayed(result: Try[Option[Offset]]) extends Command
  private final case class ListenerStopped(result: Try[Offset])  extends Command

  /**
    * Replays the materializer when it is replayable, then listens until it is told to stop. On a stop during the
    * replay it stops once the replay has finished, because a replay cannot be stopped.
    */
  private[eventmaterializers] def runner(materializer: StoppableResumable): Behavior[Command] =
    Behaviors.setup { context =>
      materializer match {
        case replayable: ResumableReplayable =>
          context.log.info("Replaying event materializer {}", materializer.viewIdentifier)
          context.pipeToSelf(replayable.replayEvents())(result => Replayed(result.map(Some(_))))
          replaying(context, materializer, stopRequested = false)
        case _ =>
          listening(context, materializer, None)
      }
    }

  private def replaying(
    context: ActorContext[Command],
    materializer: StoppableResumable,
    stopRequested: Boolean
  ): Behavior[Command] =
    Behaviors.receiveMessage {
      case Stop                                  => replaying(context, materializer, stopRequested = true)
      case Replayed(Success(_)) if stopRequested => Behaviors.stopped
      case Replayed(Success(offset))             => listening(context, materializer, offset)
      case Replayed(Failure(cause))              =>
        context.log.error(s"Replay of event materializer ${materializer.viewIdentifier} failed", cause)
        Behaviors.stopped
      case ListenerStopped(_) => Behaviors.same
    }

  private def listening(
    context: ActorContext[Command],
    materializer: StoppableResumable,
    startOffset: Option[Offset]
  ): Behavior[Command] = {
    context.log.info("Event materializer {} listens for events", materializer.viewIdentifier)
    val (killSwitch, stopped) = materializer.registerListenerWithKillSwitch(startOffset)
    context.pipeToSelf(stopped)(ListenerStopped(_))
    listeningUntilStopped(context, materializer, killSwitch)
  }

  private def listeningUntilStopped(
    context: ActorContext[Command],
    materializer: StoppableResumable,
    killSwitch: UniqueKillSwitch
  ): Behavior[Command] =
    Behaviors.receiveMessage {
      case Stop =>
        killSwitch.shutdown()
        Behaviors.receiveMessage {
          case ListenerStopped(_) => Behaviors.stopped
          case _                  => Behaviors.same
        }
      case ListenerStopped(Failure(cause)) =>
        context.log.error(s"Event materializer ${materializer.viewIdentifier} stopped with a failure", cause)
        Behaviors.stopped
      case ListenerStopped(Success(_)) => Behaviors.stopped
      case Replayed(_)                 => Behaviors.same
    }
}
