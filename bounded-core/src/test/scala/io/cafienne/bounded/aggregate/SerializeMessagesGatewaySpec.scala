/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.aggregate

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.actor.typed.Scheduler
import org.apache.pekko.actor.typed.scaladsl.adapter._
import org.apache.pekko.actor.testkit.typed.scaladsl.TestProbe
import org.apache.pekko.util.Timeout
import com.typesafe.config.ConfigFactory
import io.cafienne.bounded.aggregate.typed.DefaultTypedCommandGateway
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.time.OffsetDateTime
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

/**
  * With `serialize-messages = on` Pekko serializes every message, also inside one JVM. The gateways' own messages are
  * exempt, so only the application's commands and replies need a serializer (here Java serialization).
  */
class SerializeMessagesGatewaySpec extends AnyWordSpec with Matchers with ScalaFutures with BeforeAndAfterAll {

  private val system = ActorSystem(
    "SerializeMessagesGatewaySpec",
    ConfigFactory.parseString("""
      pekko.actor.serialize-messages = on
      pekko.actor.allow-java-serialization = on
      pekko.actor.warn-about-java-serializer-usage = off
      pekko.persistence.journal.plugin = "pekko.persistence.journal.inmem"
      pekko.persistence.snapshot-store.plugin = ""
    """)
  )

  implicit val timeout: Timeout                        = Timeout(10.seconds)
  implicit val ec: ExecutionContext                    = system.dispatcher
  implicit val scheduler: Scheduler                    = system.toTyped.scheduler
  implicit override val patienceConfig: PatienceConfig = PatienceConfig(10.seconds, 50.millis)

  override protected def afterAll(): Unit = system.terminate(): Unit

  "The typed gateway with serialize-messages on" should {
    "deliver commands and replies" in {
      import TypedSimpleAggregate._
      implicit val validator: ValidateableCommand[SimpleAggregateCommand] =
        (cmd: SimpleAggregateCommand) => Future.successful(cmd)
      val gateway = new DefaultTypedCommandGateway[SimpleAggregateCommand](system.toTyped, new SimpleAggregateManager())
      val probe   = TestProbe[Response]()(system.toTyped)

      gateway.tell(Create("serialized-1", AggregateCommandMetaData(OffsetDateTime.now(), None), probe.ref)).futureValue
      probe.expectMessage(OK)
      gateway.shutdown().futureValue
    }
  }

  "The classic gateway with serialize-messages on" should {
    "deliver commands and replies" in {
      import ClassicSimpleAggregate._
      implicit val validator: ValidateableCommand[Create] = (cmd: Create) => Future.successful(cmd)
      val gateway = new RouterCommandGateway(system, 1.minute, new ClassicSimpleAggregateCreator(system))

      gateway.sendAndAsk(Create("serialized-2")).futureValue shouldBe Ok(List(Created("serialized-2")))
    }
  }
}
