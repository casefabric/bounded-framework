/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

//package io.cafienne.bounded.test
//
////import org.apache.pekko.persistence.inmem.extension.{InMemoryJournalStorage, InMemorySnapshotStorage, StorageExtension}
//import org.apache.pekko.persistence.journal.inmem.InmemJournal
//import org.apache.pekko.testkit.{TestKit, TestProbe}
//import org.scalatest.{BeforeAndAfterEach, Suite}
//
//trait ClearStorageAfterEach extends BeforeAndAfterEach {
//  this: TestKit with Suite =>
//
//  override protected def beforeEach(): Unit = {
//    val tp = TestProbe()
//    tp.send(StorageExtension(system).journalStorage, InMemoryJournalStorage.ClearJournal)
//    tp.expectMsg(org.apache.pekko.actor.Status.Success(""))
//    tp.send(StorageExtension(system).snapshotStorage, InMemorySnapshotStorage.ClearSnapshots)
//    tp.expectMsg(org.apache.pekko.actor.Status.Success(""))
//    super.beforeEach()
//  }
//}
