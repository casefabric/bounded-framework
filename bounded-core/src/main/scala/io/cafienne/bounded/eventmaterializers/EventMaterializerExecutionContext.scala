/*
 * Copyright (C) 2016-2020 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.eventmaterializers

object EventMaterializerExecutionContext {
  implicit val executionContext =
    scala.concurrent.ExecutionContext.Implicits.global
}
