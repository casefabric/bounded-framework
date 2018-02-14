// Copyright (C) 2018 the original author or authors.
// See the LICENSE file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package io.cafienne.bounded.akka.persistence.leveldb

import akka.actor.{ActorPath, RootActorPath, Address}
import akka.persistence.query.journal.leveldb.scaladsl.LeveldbReadJournal

object SharedJournal {

  val name: String = LeveldbReadJournal.Identifier

  def pathFor(address: Address): ActorPath =
    RootActorPath(address) / "user" / name
}
