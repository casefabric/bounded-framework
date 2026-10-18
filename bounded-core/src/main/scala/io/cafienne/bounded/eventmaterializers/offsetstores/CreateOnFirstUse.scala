/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.eventmaterializers.offsetstores

import scala.concurrent.{ExecutionContext, Future}

/**
  * Runs `create` (e.g. creating the offset table) before the first operation of an offset store, without
  * blocking its constructor. A failed creation is tried again by the next operation.
  *
  * `create` must be idempotent (`CREATE TABLE IF NOT EXISTS`). Several stores on one database create the table
  * concurrently, and the database can fail the creates that lose the race (Postgres fails them with a unique
  * violation). A failed create is therefore run once more straight away: by then the table exists and it succeeds.
  */
private[offsetstores] final class CreateOnFirstUse(create: () => Future[Unit])(implicit ec: ExecutionContext) {
  private var created: Future[Unit] = _

  def apply(): Future[Unit] = synchronized {
    if (created == null || created.value.exists(_.isFailure)) {
      created = create().recoverWith { case _ => create() }
    }
    created
  }
}
