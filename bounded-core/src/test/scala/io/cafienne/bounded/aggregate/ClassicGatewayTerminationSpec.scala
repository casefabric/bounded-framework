/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.aggregate

import org.apache.pekko.actor.{ActorSystem, Props}
import org.apache.pekko.pattern.AskTimeoutException
import org.apache.pekko.persistence.testkit.PersistenceTestKitPlugin
import org.apache.pekko.persistence.testkit.scaladsl.PersistenceTestKit
import org.apache.pekko.testkit.TestKit
import org.apache.pekko.util.Timeout
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import scala.annotation.nowarn
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

/**
  * The classic gateways forget an aggregate that stopped, so the next command for it starts a fresh actor.
  */
@nowarn("cat=deprecation") // DefaultCommandGateway
class ClassicGatewayTerminationSpec
    extends TestKit(
      ActorSystem(
        "ClassicGatewayTerminationSpec",
        PersistenceTestKitPlugin.config.withFallback(ConfigFactory.parseString("""
    pekko.actor.warn-about-java-serializer-usage = off
    pekko.coordinated-shutdown.terminate-actor-system = off
    pekko.coordinated-shutdown.run-by-actor-system-terminate = off
    pekko.coordinated-shutdown.run-by-jvm-shutdown-hook = off
    """))
      )
    )
    with AnyFlatSpecLike
    with ScalaFutures
    with Eventually
    with Matchers
    with BeforeAndAfterAll {

  import ClassicSimpleAggregate._

  implicit val timeout: Timeout     = Timeout(1.second)
  implicit val ec: ExecutionContext = system.dispatcher
  implicit val patience: PatienceConfig =
    PatienceConfig(timeout = Span(5, Seconds), interval = Span(50, Millis))

  val persistenceTestKit: PersistenceTestKit = PersistenceTestKit(system)

  implicit val createValidator: ValidateableCommand[Create] = new ValidateableCommand[Create] {
    override def validate(cmd: Create): Future[Create] = Future.successful(cmd)
  }

  implicit val stopValidator: ValidateableCommand[Stop] = new ValidateableCommand[Stop] {
    override def validate(cmd: Stop): Future[Stop] = Future.successful(cmd)
  }

  /** Counts the actors created per aggregate id. */
  class CountingCreator extends AggregateRootCreator {
    private val created = new ConcurrentHashMap[String, AtomicInteger]()

    def actorsCreated(id: String): Int = Option(created.get(id)).fold(0)(_.get)

    override def props(aggregateRootId: String): Props = {
      created.computeIfAbsent(aggregateRootId, _ => new AtomicInteger()).incrementAndGet()
      Props(classOf[ClassicSimpleAggregate], aggregateRootId)
    }
  }

  def gateways(creator: CountingCreator): Seq[(String, CommandGateway)] = Seq(
    "RouterCommandGateway"  -> new RouterCommandGateway(system, 1.minute, creator),
    "DefaultCommandGateway" -> new DefaultCommandGateway(system, creator)
  )

  gateways(new CountingCreator).map(_._1).zipWithIndex.foreach {
    case (gatewayName, index) =>
      behavior of gatewayName

      it should "start a fresh aggregate for the next command after its recovery failed" in {
        val creator     = new CountingCreator
        val gateway     = gateways(creator)(index)._2
        val aggregateId = s"recovery-failure-$index"
        persistenceTestKit.failNextRead(aggregateId)

        // the aggregate stops while recovering, before it handles this command
        gateway.sendAndAsk(Create(aggregateId)).failed.futureValue shouldBe an[AskTimeoutException]

        gateway.sendAndAsk(Create(aggregateId)).futureValue shouldBe Ok(List(Created(aggregateId)))
        creator.actorsCreated(aggregateId) shouldBe 2
      }

      it should "start a fresh aggregate for the next command after the aggregate stopped itself" in {
        val creator     = new CountingCreator
        val gateway     = gateways(creator)(index)._2
        val aggregateId = s"stopped-itself-$index"
        gateway.sendAndAsk(Stop(aggregateId)).futureValue shouldBe Ok(List.empty)

        eventually {
          gateway.sendAndAsk(Create(aggregateId)).futureValue shouldBe Ok(List(Created(aggregateId)))
        }
        creator.actorsCreated(aggregateId) shouldBe 2
      }

      it should "start one actor for concurrent first commands to the same aggregate" in {
        val creator     = new CountingCreator
        val gateway     = gateways(creator)(index)._2
        val aggregateId = s"concurrent-$index"

        val replies = Future.sequence((1 to 20).map(_ => gateway.sendAndAsk(Create(aggregateId)))).futureValue

        replies.toSet shouldBe Set(Ok(List(Created(aggregateId))))
        creator.actorsCreated(aggregateId) shouldBe 1
      }
  }

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system, 30.seconds, verifySystemShutdown = true)
  }
}
