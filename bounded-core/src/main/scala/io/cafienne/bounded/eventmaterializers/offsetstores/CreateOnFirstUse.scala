/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.eventmaterializers.offsetstores

import java.util.concurrent.atomic.AtomicReference

import scala.concurrent.Future

/**
  * Runs `create` (e.g. creating the offset table) before the first operation of an offset store, without
  * blocking its constructor. A failed creation is tried again by the next operation.
  */
private[offsetstores] final class CreateOnFirstUse(create: () => Future[Unit]) {
  private val created = new AtomicReference[Future[Unit]]()

  def apply(): Future[Unit] = {
    val current = created.get
    if (current != null && !current.value.exists(_.isFailure)) current
    else {
      val attempt = create()
      created.set(attempt)
      attempt
    }
  }
}
