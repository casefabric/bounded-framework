/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.eventmaterializers

import com.typesafe.config.ConfigFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

class RetrySettingsSpec extends AnyFlatSpec with Matchers {

  private def retrySettings(restart: String) =
    EventStreamHandling.RetrySettings(EventStreamHandling.restartSettings(ConfigFactory.parseString(restart)))

  behavior of "The replay retry settings"

  they should "take the restart settings that RetrySupport accepts as they are" in {
    retrySettings(
      "min-backoff = 1s, max-backoff = 30s, random-factor = 0.2, max-restarts = 10, max-restarts-within = 10m"
    ) shouldBe EventStreamHandling.RetrySettings(10, 1.second, 30.seconds, 0.2)
  }

  they should "map the restart settings that only RestartSource accepts" in {
    retrySettings(
      "min-backoff = 0s, max-backoff = 0s, random-factor = 1.5, max-restarts = -1, max-restarts-within = 10m"
    ) shouldBe EventStreamHandling.RetrySettings(Int.MaxValue, 1.millisecond, 1.millisecond, 1.0)
  }
}
