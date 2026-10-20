/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.eventmaterializers.offsetstores

import org.testcontainers.DockerClientFactory

import scala.util.Try

/**
  * Specs that need a database in a container are canceled when Docker is not available.
  */
object DockerTests {
  lazy val dockerAvailable: Boolean = Try(DockerClientFactory.instance().isDockerAvailable).getOrElse(false)
}
