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
package bounded.test

import akka.NotUsed
import akka.actor.ActorSystem
import akka.persistence.inmemory.extension.{
  InMemoryJournalStorage,
  InMemorySnapshotStorage,
  StorageExtension
}
import akka.persistence.query.scaladsl._
import akka.persistence.query.{EventEnvelope, PersistenceQuery}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestProbe

import scala.concurrent.Await
import scala.concurrent.duration._

trait TestJournalProvider {
  implicit def journalActorSystem: ActorSystem = ???

  implicit val materializer: ActorMaterializer = ActorMaterializer()

  private lazy val readJournal = PersistenceQuery(journalActorSystem)
    .readJournalFor("inmemory-read-journal")
    .asInstanceOf[ReadJournal with CurrentPersistenceIdsQuery with CurrentEventsByPersistenceIdQuery with CurrentEventsByTagQuery with EventsByPersistenceIdQuery with EventsByTagQuery]

  protected def resetJournal(): Unit = {
    val tp = TestProbe()
    tp.send(StorageExtension(journalActorSystem).journalStorage,
            InMemoryJournalStorage.ClearJournal)
    tp.expectMsg(akka.actor.Status.Success(""))
    tp.send(StorageExtension(journalActorSystem).snapshotStorage,
            InMemorySnapshotStorage.ClearSnapshots)
    tp.expectMsg(akka.actor.Status.Success(""))
  }

  protected def waitForEvent(persistenceId: String,
                             maxWaitTimeMillis: Long): Boolean = {
    var hasEvent = false

    val start = System.currentTimeMillis()
    while (!hasEvent && ((System
             .currentTimeMillis() - start) < maxWaitTimeMillis)) {
      Thread.sleep(20) // Needed since it takes a while before inmemory-db akka persistece plugin makes event(s) available
      val eventsStream: Source[EventEnvelope, NotUsed] = readJournal
        .currentEventsByPersistenceId(persistenceId, 0L, Long.MaxValue)
      val future = eventsStream.take(1).runWith(Sink.seq)
      hasEvent = Await.result(future, 1.seconds).nonEmpty
    }

    hasEvent
  }
}
