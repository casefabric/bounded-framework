/*
 * Copyright (C) 2016-2020 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.test.typed

import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.typed.{ActorRef, Scheduler}
import akka.util.Timeout
import io.cafienne.bounded.aggregate.typed.{TypedAggregateRootManager, TypedCommandGateway}
import io.cafienne.bounded.aggregate.{DomainCommand, ValidateableCommand}

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

class TestTypedCommandGateway[T <: DomainCommand](
  testKit: ActorTestKit,
  aggregateRootCreator: TypedAggregateRootManager[T]
)(
  implicit timeout: Timeout,
  ec: ExecutionContext,
  scheduler: Scheduler
) extends TypedCommandGateway[T] {
  import akka.actor.typed.scaladsl.AskPattern._

  val aggregates = mutable.Map[String, ActorRef[T]]()

  override def ask[Res](aggregateRootId: String, replyTo: ActorRef[Res] => T)(
    implicit validator: ValidateableCommand[T]
  ): Future[Res] = {
    val aggregateRoot =
      aggregates.getOrElseUpdate(aggregateRootId, testKit.spawn(aggregateRootCreator.behavior(aggregateRootId)))
    aggregateRoot.ask(replyTo)
  }

  override def tell(command: T)(implicit validator: ValidateableCommand[T]): Future[_] = {
    val aggregateRoot = aggregates.getOrElseUpdate(
      command.aggregateRootId,
      testKit.spawn(aggregateRootCreator.behavior(command.aggregateRootId))
    )
    Future { aggregateRoot.tell(command) }
  }

  override def shutdown(): Future[Unit] = {
    aggregates.values.foreach { aggregateActor => testKit.stop(aggregateActor) }
    Future.successful()
  }

}
