/*
 * Copyright (C) 2016-2018 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.test

import java.util.concurrent.atomic.AtomicInteger
import scala.reflect.runtime.universe

import akka.actor._
import akka.pattern.ask
import akka.testkit.TestProbe
import akka.util.Timeout
import io.cafienne.bounded.aggregate.AggregateRootActor.GetState
import io.cafienne.bounded.aggregate._

import scala.concurrent.Future
import scala.concurrent.duration.Duration

/**
  * Allows to test an aggregate root in isolation by creation of the aggregate root and the historic events in order
  * to create the state you want to test.
  * Thereafter a command will result in no, a single or more events that can be asserted.
  * Next to that you have the possibility to assert the inside state of the Aggregate Root.
  *
  * An example of the use can be found in the cargo sample and looks like:
  * @example
  * {{{
  * val ar = TestableAggregateRoot
  *      .given[Cargo](cargoId2)
  *      .when(PlanCargo(metaData, cargoId2, trackingId, routeSpecification))
  *
  *    ar.events should contain(CargoPlanned(metaData, cargoId2, trackingId, routeSpecification))
  *    val targetState = CargoAggregateState(trackingId, routeSpecification)
  *    ar.currentState map { state => assert(state == targetState)}
  * }}}
  */
object TestableAggregateRoot {

  /** Construct a test for a specific aggregate root that has a specific initial state
    *
    * @param id The aggregate root ID that is used for testing
    * @param evt A single or more DomainEvents that will be used to create the initial state of the aggregate root
    * @tparam A The Aggregate Root Type that is tested.
    * @return a TestableAggregateRoot instance that is initialized and available to give a DomainCommand to.
    */
  def given[A <: AggregateRootActor](
    id: AggregateRootId,
    evt: DomainEvent*
  )(implicit system: ActorSystem, timeout: Timeout, ctag: reflect.ClassTag[A]): TestableAggregateRoot[A] = {
    new TestableAggregateRoot[A](id, evt)
  }

  /** Construct a test for a specific aggregate root that hos no initial state. This can be used to test creation of the
    * aggregate root and see if it is constructed with the right initial state.
    *
    * @param id The aggregate root ID that is used for testing
    * @tparam A The Aggregate Root Type that is tested.
    * @return a TestableAggregateRoot instance that is initialized and available to give a DomainCommand to.
    */
  def given[A <: AggregateRootActor](
    id: AggregateRootId
//    aggregateRootCreator: AggregateRootCreator
  )(implicit system: ActorSystem, timeout: Timeout, ctag: reflect.ClassTag[A]): TestableAggregateRoot[A] = {

    new TestableAggregateRoot[A](id, Seq.empty[DomainEvent])
  }

  //The tested aggregate root makes use of an additional counter in the id in order to prevent collision of parallel running tests.
  private val atomicCounter: AtomicInteger = new AtomicInteger()
  private def testId(id: AggregateRootId): AggregateRootId =
    TestId(id.idAsString + "-" + atomicCounter.getAndIncrement().toString)

  private case class TestId(id: String) extends AggregateRootId {
    override def idAsString: String = id

    override def toString: String = this.idAsString
  }
}

class TestableAggregateRoot[A <: AggregateRootActor] private (id: AggregateRootId, evt: Seq[DomainEvent])(
  implicit system: ActorSystem,
  timeout: Timeout,
  ctag: reflect.ClassTag[A]
) {

  import TestableAggregateRoot.testId
  final val arTestId = testId(id)

  private val storeEventsActor =
    system.actorOf(Props(classOf[CreateEventsInStoreActor], arTestId), "create-events-actor")
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

  private def aggregateRootCreator(): AggregateRootCreator = {
    val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
    val module        = runtimeMirror.staticModule(ctag.runtimeClass.getCanonicalName)
    val obj           = runtimeMirror.reflectModule(module)
    obj.instance.asInstanceOf[AggregateRootCreator]
  }

  private def createActor[B <: AggregateRootActor](id: AggregateRootId) = {
    handledEvents = List.empty
    system.actorOf(aggregateRootCreator.props(arTestId), s"test-aggregate-$arTestId")
  }

  /**
    * After initialization of the aggregate root, the when allows to send a command and check thereafter what events are
    * created.
    *
    * @param command The DomainCommand that the aggregate root needs to process.
    * @return This initialized TestableAggregateRoot that processed the command.
    */
  def when(command: DomainCommand): TestableAggregateRoot[A] = {
    //TODO IT maybe a better idea to actually return the results in a testable matter. (HOW?)
    if (command.id != id)
      throw new IllegalArgumentException(
        s"Command for Aggregate Root ${command.id} cannot be handled by this aggregate root with id $id"
      )
    aggregateRootActor = aggregateRootActor.fold(Some(createActor[A](arTestId)))(r => Some(r))
    val aggregateRootProbe = TestProbe()
    aggregateRootProbe watch aggregateRootActor.get

    aggregateRootProbe.send(aggregateRootActor.get, command)

    val events =
      aggregateRootProbe
        .expectMsgPF[Seq[DomainEvent]](duration, "reply with events") {
          case Ok(events) => events
        }

    handledEvents ++= events
    this
  }

  /**
    * Fetch the current state of the Aggregate Root Actor.
    * As the state is stored in the actor, the method will ask the actor for its' internal state so the method
    * returns a Future that will complete when the state is returned.
    *
    * @return Future with the AggregateState as defined for this Aggregate Root.
    */
  def currentState: Future[AggregateState] = {
    aggregateRootActor.fold(Future.failed[AggregateState](new IllegalStateException("")))(
      actor => (actor ? GetState).mapTo[AggregateState]
    )
  }

  /**
    * Give the events that are created by the command that was given to the aggregate root by when.
    * @return List of DomainEvents that is created by the command.
    */
  def events: List[DomainEvent] = {
    handledEvents
  }

  override def toString: String = {
    s"Aggregate Root ${ctag.runtimeClass.getSimpleName} ${id.idAsString}"
  }

}
