/*
 * Copyright (C) 2016-2018 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.aggregate

class CommandValidationException(msg: String) extends Exception(msg) {
  def this(msg: String, cause: Throwable) = {
    this(msg)
    initCause(cause)
  }
}

object CommandValidationException {
  def apply(reason: String): CommandValidationException =
    new CommandValidationException(reason)
  def apply(reason: String, cause: Throwable): CommandValidationException =
    new CommandValidationException(reason, cause)

  def unapply(e: CommandValidationException): Option[(String, Option[Throwable])] =
    Some((e.getMessage, Option(e.getCause)))
}
