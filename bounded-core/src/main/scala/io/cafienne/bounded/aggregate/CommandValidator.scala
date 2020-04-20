/*
 * Copyright (C) 2016-2020 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.aggregate

import scala.concurrent.Future

trait ValidateableCommand[T] {
  def validate(cmd: T): Future[T]
}

object CommandValidator {
  def validate[T](v: T)(implicit validator: ValidateableCommand[T]): Future[T] =
    validator.validate(v)
}
