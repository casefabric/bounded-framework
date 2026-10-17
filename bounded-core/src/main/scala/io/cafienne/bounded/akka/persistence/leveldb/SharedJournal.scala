/*
 * Copyright (C) 2016-2025 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.akka.persistence.leveldb

import org.apache.pekko.actor.{ActorPath, RootActorPath, Address}
import org.apache.pekko.persistence.query.journal.leveldb.scaladsl.LeveldbReadJournal

object SharedJournal {

  val name: String = LeveldbReadJournal.Identifier

  def pathFor(address: Address): ActorPath =
    RootActorPath(address) / "user" / name
}
