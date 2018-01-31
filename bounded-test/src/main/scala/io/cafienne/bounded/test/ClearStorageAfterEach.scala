// Copyright (C) 2018 the original author or authors.
// See the LICENCE.txt file distributed with this work for additional
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
package io.cafienne.bounded.test

import akka.persistence.inmemory.extension.{
  InMemoryJournalStorage,
  InMemorySnapshotStorage,
  StorageExtension
}
import akka.testkit.{TestKit, TestProbe}
import org.scalatest.{BeforeAndAfterEach, Suite}

trait ClearStorageAfterEach extends BeforeAndAfterEach {
  this: TestKit with Suite =>

  override protected def beforeEach(): Unit = {
    val tp = TestProbe()
    tp.send(StorageExtension(system).journalStorage,
            InMemoryJournalStorage.ClearJournal)
    tp.expectMsg(akka.actor.Status.Success(""))
    tp.send(StorageExtension(system).snapshotStorage,
            InMemorySnapshotStorage.ClearSnapshots)
    tp.expectMsg(akka.actor.Status.Success(""))
    super.beforeEach()
  }
}
