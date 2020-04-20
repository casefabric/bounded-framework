/*
 * Copyright (C) 2016-2020 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.akka.persistence.leveldb

import akka.actor.{ActorPath, RootActorPath, Address}
import akka.persistence.query.journal.leveldb.scaladsl.LeveldbReadJournal

object SharedJournal {

  val name: String = LeveldbReadJournal.Identifier

  def pathFor(address: Address): ActorPath =
    RootActorPath(address) / "user" / name
}
