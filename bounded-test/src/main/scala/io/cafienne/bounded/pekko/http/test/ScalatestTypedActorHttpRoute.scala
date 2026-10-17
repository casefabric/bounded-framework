/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.pekko.http.test

import org.apache.pekko.actor.testkit.typed.scaladsl.{ActorTestKit, ActorTestKitBase}
import org.apache.pekko.actor.{ActorSystem, Scheduler}
import org.apache.pekko.http.scaladsl.testkit.ScalatestRouteTest
import org.apache.pekko.util.Timeout
import org.scalatest.Suite

//Thanks babloo80 -> https://github.com/akka/akka-http/issues/2036
trait ScalatestTypedActorHttpRoute extends ScalatestRouteTest { this: Suite =>
  import org.apache.pekko.actor.typed.scaladsl.adapter._

  var typedTestKit
    : ActorTestKit                  = _ //val init causes createActorSystem() to cause NPE when typedTestKit.system is called in createActorSystem().
  implicit def timeout: Timeout     = typedTestKit.timeout
  implicit def scheduler: Scheduler = typedTestKit.scheduler.toClassic

  protected override def createActorSystem(): ActorSystem = {
    typedTestKit = ActorTestKit(ActorTestKitBase.testNameFromCallStack())
    typedTestKit.system.toClassic
  }

  override def cleanUp(): Unit = {
    super.cleanUp()
    typedTestKit.shutdownTestKit()
  }
}
