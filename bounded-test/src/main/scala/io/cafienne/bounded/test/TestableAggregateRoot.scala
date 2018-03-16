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
package io.cafienne.bounded.test

import java.util.concurrent.atomic.AtomicInteger

import akka.actor._
import akka.pattern.ask
import akka.testkit.TestProbe
import akka.util.Timeout
import io.cafienne.bounded.aggregate.AggregateRootActor.GetState
import io.cafienne.bounded.aggregate._

import scala.concurrent.Future
import scala.concurrent.duration.Duration

class TestableAggregateRoot[A <: AggregateRootActor](id: AggregateRootId, evt: Seq[DomainEvent])
                                                    (implicit system: ActorSystem, timeout: Timeout, ctag: reflect.ClassTag[A]) {

  import TestableAggregateRoot.testId
  final val arTestId = testId(id)

  private val storeEventsActor = system.actorOf(Props(classOf[CreateEventsInStoreActor], arTestId), "create-events-actor")
  private var handledEvents: List[DomainEvent] = List.empty

  implicit val duration: Duration = timeout.duration

  private val testProbe = TestProbe()
  testProbe watch storeEventsActor

  evt foreach { event =>
    testProbe.send(storeEventsActor, event)
    testProbe.expectMsgAllConformingOf(classOf[DomainEvent])
  }
  storeEventsActor ! PoisonPill
  testProbe.expectTerminated(storeEventsActor)

  private var aggregateRootActor: Option[ActorRef] = None

  private def createActor[B <: AggregateRootActor](id: AggregateRootId) = {
    handledEvents = List.empty
    system.actorOf(Props(ctag.runtimeClass, arTestId), s"test-aggregate-$arTestId")
  }

  def when(command: DomainCommand): TestableAggregateRoot[A] = {
    if (command.id != id) throw new IllegalArgumentException(s"Command for Aggregate Root ${command.id} cannot be handled by this aggregate root with id $id")
    aggregateRootActor = aggregateRootActor.fold(Some(createActor[A](arTestId)))(r => Some(r))
    val aggregateRootProbe = TestProbe()
    aggregateRootProbe watch aggregateRootActor.get

    aggregateRootProbe.send(aggregateRootActor.get, command)

    val events =
      aggregateRootProbe.expectMsgPF[Seq[DomainEvent]](duration, "reply with events") {
        case Ok(events) => events
      }

    handledEvents ++= events
    this
  }

  def currentState: Future[AggregateState] = {
    aggregateRootActor.fold(Future.failed[AggregateState](new IllegalStateException("")))(actor => (actor ? GetState).mapTo[AggregateState])
  }

  def events: List[DomainEvent] = {
    handledEvents
  }

  override def toString: String = {
    s"Aggregate Root ${ctag.runtimeClass.getSimpleName} ${id.idAsString}"
  }


}

object TestableAggregateRoot {

  def given[A <: AggregateRootActor](id: AggregateRootId, evt: DomainEvent*)
                                    (implicit system: ActorSystem, timeout: Timeout, ctag: reflect.ClassTag[A]): TestableAggregateRoot[A] = {
    new TestableAggregateRoot[A](id, evt)
  }

  //The tested aggregate root makes use of an additional counter in the id in order to prevent collision of parallel running tests.
  private val atomicCounter: AtomicInteger = new AtomicInteger()
  private def testId(id: AggregateRootId): AggregateRootId = TestId(id.idAsString + "-" + atomicCounter.getAndIncrement().toString)

  private case class TestId(id: String) extends AggregateRootId {
    override def idAsString: String = id

    override def toString: String = this.idAsString
  }
}

