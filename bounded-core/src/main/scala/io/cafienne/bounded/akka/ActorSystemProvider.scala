/*
 * Copyright (C) 2016-2021 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.akka

import akka.actor.ActorSystem

trait ActorSystemProvider {
  implicit def system: ActorSystem
}
