/*
 * Copyright (C) 2016-2022 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.akka.http.test

import akka.actor.testkit.typed.scaladsl.{ActorTestKit, ActorTestKitBase}
import akka.actor.{ActorSystem, Scheduler}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.util.Timeout
import org.scalatest.Suite

//Thanks babloo80 -> https://github.com/akka/akka-http/issues/2036
trait ScalatestTypedActorHttpRoute extends ScalatestRouteTest { this: Suite =>
  import akka.actor.typed.scaladsl.adapter._

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
