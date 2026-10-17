/*
 * Copyright (C) 2016-2025 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.akka

import org.apache.pekko.actor.ActorSystem

trait ActorSystemProvider {
  implicit def system: ActorSystem
}
