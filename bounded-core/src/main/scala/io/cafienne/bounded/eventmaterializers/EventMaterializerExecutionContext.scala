/*
 * Copyright (C) 2016-2023 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.eventmaterializers

import scala.concurrent.ExecutionContext

object EventMaterializerExecutionContext {
  implicit val executionContext: ExecutionContext =
    scala.concurrent.ExecutionContext.Implicits.global
}
