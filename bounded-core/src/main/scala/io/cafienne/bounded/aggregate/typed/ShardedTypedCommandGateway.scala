/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.aggregate.typed

import org.apache.pekko.actor.typed.{ActorRef, ActorSystem, Terminated}
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.actor.typed.scaladsl.adapter._
import org.apache.pekko.cluster.sharding.{ClusterSharding => ClassicClusterSharding, ShardRegion}
import org.apache.pekko.cluster.sharding.typed.ClusterShardingSettings
import org.apache.pekko.cluster.sharding.typed.scaladsl.{ClusterSharding, Entity, EntityTypeKey}
import org.apache.pekko.util.Timeout
import io.cafienne.bounded.aggregate.{CommandValidator, DomainCommand, ValidateableCommand}

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.concurrent.{ExecutionContext, Future, Promise}

/**
  * A TypedCommandGateway for a cluster: it runs the aggregates with Cluster Sharding, so each aggregate runs on one
  * node of the cluster at a time, wherever its commands come from. Calling code can switch to it from
  * `DefaultTypedCommandGateway` without changes.
  *
  * Every node that handles commands creates this gateway for the same manager, so that the node takes part in
  * sharding the aggregate type. Commands and replies then cross nodes: they need a serializer binding (not Java
  * serialization), and so do the events, which every journal needs anyway.
  *
  * Sharding stops an aggregate after `aggregateMayIdleFor` without commands, and buffers the commands for an
  * aggregate that is being stopped or moved to another node.
  *
  * @param system a typed actor system with `pekko.actor.provider = cluster`
  * @param aggregateRootCreator creates the aggregates; the name of its `entityTypeKey` is the sharding type name
  * @param aggregateMayIdleFor the time an aggregate may be idle before sharding stops it
  * @param timeout timeout of `ask`
  * @tparam Cmd the commands of the aggregate
  */
class ShardedTypedCommandGateway[Cmd <: DomainCommand: ClassTag](
  system: ActorSystem[_],
  aggregateRootCreator: TypedAggregateRootManager[Cmd],
  aggregateMayIdleFor: FiniteDuration = 6.minutes
)(implicit timeout: Timeout, ec: ExecutionContext)
    extends TypedCommandGateway[Cmd] {

  // The manager declares an EntityTypeKey[_]; sharding needs one for Cmd, which a key of the same name is.
  val entityTypeKey: EntityTypeKey[Cmd] = EntityTypeKey[Cmd](aggregateRootCreator.entityTypeKey.name)

  private val sharding = ClusterSharding(system)

  private val shardingSettings = {
    val settings = ClusterShardingSettings(system)
    settings.withPassivationStrategy(
      settings.passivationStrategySettings.withIdleEntityPassivation(aggregateMayIdleFor)
    )
  }

  sharding.init(
    Entity(entityTypeKey)(entityContext => aggregateRootCreator.behaviorFor(entityContext.entityId))
      .withSettings(shardingSettings)
  )

  override def ask[Res](aggregateRootId: String, replyTo: ActorRef[Res] => Cmd)(
    implicit validator: ValidateableCommand[Cmd]
  ): Future[Res] = {
    // As in DefaultTypedCommandGateway: the command is built with the reply ref of an ask on the ignore ref, then
    // validated, and the reply to the validated command completes the ask.
    import org.apache.pekko.actor.typed.scaladsl.AskPattern._
    implicit val scheduler: org.apache.pekko.actor.typed.Scheduler = system.scheduler
    var command: Option[Cmd]                                       = None
    val response = system.ignoreRef[ActorRef[Res]].ask[Res] { replyRef =>
      command = Some(replyTo(replyRef))
      replyRef
    }
    command match {
      case Some(cmd) =>
        CommandValidator
          .validate(cmd)
          .map(validatedCommand => sharding.entityRefFor(entityTypeKey, aggregateRootId) ! validatedCommand)
          .flatMap(_ => response)
      case None =>
        Future.failed(new IllegalStateException(s"No command built for aggregate $aggregateRootId"))
    }
  }

  override def tell(command: Cmd)(implicit validator: ValidateableCommand[Cmd]): Future[_] =
    CommandValidator
      .validate(command)
      .map(validatedCommand =>
        sharding.entityRefFor(entityTypeKey, validatedCommand.aggregateRootId) ! validatedCommand
      )

  /**
    * Hands the aggregates on this node over to the other nodes and stops this node's part of the sharding of the
    * aggregate type. The future completes when that has finished. Shutting down the actor system (or the node leaving
    * the cluster) does the same for every sharded type.
    */
  override def shutdown(): Future[Unit] = {
    val region  = ClassicClusterSharding(system.toClassic).shardRegion(entityTypeKey.name)
    val stopped = Promise[Unit]()
    system.systemActorOf(
      Behaviors.setup[Nothing] { context =>
        context.watch(region.toTyped)
        Behaviors.receiveSignal[Nothing] { case (_, Terminated(_)) =>
          stopped.trySuccess(())
          Behaviors.stopped
        }
      },
      s"sharded-gateway-shutdown-${entityTypeKey.name}-${ShardedTypedCommandGateway.nextShutdownNumber()}"
    )
    region ! ShardRegion.GracefulShutdown
    stopped.future
  }
}

object ShardedTypedCommandGateway {
  private val shutdownCount = new AtomicInteger()

  private def nextShutdownNumber(): Int = shutdownCount.incrementAndGet()
}
