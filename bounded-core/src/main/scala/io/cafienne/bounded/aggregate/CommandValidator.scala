/*
 * Copyright (C) 2016-2018 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.aggregate

import scala.concurrent.Future

trait ValidateableCommand[T <: DomainCommand] {
  def validate(cmd: T): Future[T]
}

object CommandValidator {
  def validate[T <: DomainCommand](v: T)(
      implicit validator: ValidateableCommand[T]): Future[T] =
    validator.validate(v)
}
