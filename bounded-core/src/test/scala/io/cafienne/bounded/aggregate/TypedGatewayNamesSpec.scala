/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.aggregate

import java.time.OffsetDateTime
import org.apache.pekko.actor.testkit.typed.scaladsl.{LogCapturing, ScalaTestWithActorTestKit}
import org.apache.pekko.actor.typed.Scheduler
import org.apache.pekko.util.Timeout
import io.cafienne.bounded.aggregate.typed.DefaultTypedCommandGateway
import com.typesafe.config.ConfigFactory
import org.scalatest.flatspec.AnyFlatSpecLike

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

/**
  * Aggregate ids that are not valid actor names, and gateways that share a manager class.
  */
class TypedGatewayNamesSpec
    extends ScalaTestWithActorTestKit(
      ConfigFactory.parseString("""
    pekko.persistence.journal.plugin = "pekko.persistence.journal.inmem"
    pekko.actor.warn-about-java-serializer-usage = off
    pekko.coordinated-shutdown.terminate-actor-system = off
    pekko.coordinated-shutdown.run-by-actor-system-terminate = off
    pekko.coordinated-shutdown.run-by-jvm-shutdown-hook = off
    """)
    )
    with AnyFlatSpecLike
    with LogCapturing {

  import TypedSimpleAggregate._

  implicit val commandValidator: ValidateableCommand[SimpleAggregateCommand] =
    new ValidateableCommand[SimpleAggregateCommand] {
      override def validate(cmd: SimpleAggregateCommand): Future[SimpleAggregateCommand] = Future.successful(cmd)
    }
  implicit val ec: ExecutionContext    = system.executionContext
  implicit val scheduler: Scheduler    = system.scheduler
  implicit val gatewayTimeout: Timeout = Timeout(5.seconds)
  private val commandMetaData          = AggregateCommandMetaData(OffsetDateTime.now(), None)

  private def create(gateway: DefaultTypedCommandGateway[SimpleAggregateCommand], id: String): Future[Response] =
    gateway.ask[Response](id, ref => Create(id, commandMetaData, ref))

  behavior of "DefaultTypedCommandGateway"

  it should "handle aggregate ids that are not valid actor names" in {
    val gateway = new DefaultTypedCommandGateway[SimpleAggregateCommand](system, new SimpleAggregateManager())

    Seq("a/b c", "$starts-with-dollar", "Jan Jansen", "x?y#z%").foreach { id =>
      create(gateway, id).futureValue shouldBe OK
    }
    create(gateway, "plain-id").futureValue shouldBe OK
  }

  it should "fail only the request for an aggregate it cannot create" in {
    val gateway = new DefaultTypedCommandGateway[SimpleAggregateCommand](system, new SimpleAggregateManager())

    // no actor name can be made for an empty id
    create(gateway, "").failed.futureValue.getMessage should include("must not be empty")
    create(gateway, "after-failure").futureValue shouldBe OK
  }

  it should "allow two gateways for the same manager class in one actor system" in {
    val first  = new DefaultTypedCommandGateway[SimpleAggregateCommand](system, new SimpleAggregateManager())
    val second = new DefaultTypedCommandGateway[SimpleAggregateCommand](system, new SimpleAggregateManager())

    create(first, "first-gateway").futureValue shouldBe OK
    create(second, "second-gateway").futureValue shouldBe OK
  }

  it should "give different ids different actor names" in {
    DefaultTypedCommandGateway.actorName("a/b c") shouldBe "a%2Fb+c"
    DefaultTypedCommandGateway.actorName("a+b") should not be DefaultTypedCommandGateway.actorName("a b")
  }
}
