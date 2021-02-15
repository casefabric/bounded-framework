/*
 * Copyright (C) 2016-2021 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.test

import akka.actor.{ActorRef, ActorSystem}
import akka.testkit.{TestActor, TestProbe}

object LoggingTestProbe {
  def apply()(implicit system: ActorSystem): TestProbe = {
    val probe = TestProbe()
    probe.setAutoPilot(new TestActor.AutoPilot {
      def run(sender: ActorRef, msg: Any) = {
        val other = sender.path
        val me    = probe.ref.path
        system.log.debug(s"$me received $msg from $other")
        this
      }
    })
    probe
  }
}
