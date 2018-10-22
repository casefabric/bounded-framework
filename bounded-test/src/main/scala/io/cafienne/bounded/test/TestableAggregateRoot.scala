/*
 * Copyright (C) 2016-2018 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.test

import java.util.concurrent.atomic.AtomicInteger

import io.cafienne.bounded.test.TestableAggregateRoot._

import scala.reflect.ClassTag
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

  case class CommandHandlingFailed(failure: HandlingFailure)
      extends Exception(s"Command handling failed with [$failure]")

  case class MisdirectedCommand(expectedId: AggregateRootId, command: DomainCommand)
      extends Exception(s"Expected command [$command] to target [$expectedId], not [${command.aggregateRootId}]")

  case class UnexpectedCommandHandlingSuccess(command: DomainCommand)
      extends Exception(s"Expected handling of command [$command] to fail")

  case object NoCommandsIssued extends Exception("Expected one or more commands, but got none")

  /** Construct a test for a specific aggregate root that has a specific initial state
    *
    * @param id The aggregate root ID that is used for testing
    * @param evt A single or more DomainEvents that will be used to create the initial state of the aggregate root
    * @tparam A The Aggregate Root Type that is tested.
    * @return a TestableAggregateRoot instance that is initialized and available to give a DomainCommand to.
    */
  def given[A <: AggregateRootActor[B], B <: AggregateState[B]: ClassTag](
    creator: AggregateRootCreator,
    id: AggregateRootId,
    evt: DomainEvent*
  )(implicit system: ActorSystem, timeout: Timeout, ctag: reflect.ClassTag[A]): TestableAggregateRoot[A, B] = {
    new TestableAggregateRoot[A, B](creator, id, evt)
  }

  /** Construct a test for a specific aggregate root that hos no initial state. This can be used to test creation of the
    * aggregate root and see if it is constructed with the right initial state.
    *
    * @param id The aggregate root ID that is used for testing
    * @tparam A The Aggregate Root Type that is tested.
    * @return a TestableAggregateRoot instance that is initialized and available to give a DomainCommand to.
    */
  def given[A <: AggregateRootActor[B], B <: AggregateState[B]: ClassTag](
    creator: AggregateRootCreator,
    id: AggregateRootId
  )(implicit system: ActorSystem, timeout: Timeout, ctag: reflect.ClassTag[A]): TestableAggregateRoot[A, B] = {

    new TestableAggregateRoot[A, B](creator, id, Seq.empty[DomainEvent])
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

class TestableAggregateRoot[A <: AggregateRootActor[B], B <: AggregateState[B]: ClassTag] private (
  creator: AggregateRootCreator,
  id: AggregateRootId,
  evt: Seq[DomainEvent]
)(
  implicit system: ActorSystem,
  timeout: Timeout,
  ctag: reflect.ClassTag[A]
) {

  implicit val duration: Duration              = timeout.duration
  private var handledEvents: List[DomainEvent] = List.empty

  private var lastFailure: Option[HandlingFailure] = Option.empty
  private var lastCommand: Option[DomainCommand]   = Option.empty

  import TestableAggregateRoot.testId
  final val arTestId = testId(id)

  if (evt != null && evt.nonEmpty) storeEvents(evt)
  // Start the Aggregate Root and replay to initial state
  private val aggregateRootActor: ActorRef = system.actorOf(creator.props(arTestId), s"test-aggregate-$arTestId")

  private def storeEvents(evt: Seq[DomainEvent]): Unit = {
    val storeEventsActor = system.actorOf(Props(classOf[CreateEventsInStoreActor], arTestId), "create-events-actor")
    val testProbe        = TestProbe()

    testProbe watch storeEventsActor
    evt foreach { event =>
      testProbe.send(storeEventsActor, event)
      testProbe.expectMsgAllConformingOf(classOf[DomainEvent])
    }
    storeEventsActor ! PoisonPill
    testProbe.expectTerminated(storeEventsActor)
    ()
  }

  /**
    * After initialization of the aggregate root, the when allows to send a command and check thereafter what events are
    * created.
    *
    * @param command The DomainCommand that the aggregate root needs to process.
    * @return This initialized TestableAggregateRoot that processed the command.
    */
  def when(command: DomainCommand): TestableAggregateRoot[A, B] = {
    if (command.aggregateRootId != id)
      throw MisdirectedCommand(id, command)

    val mediator = TestProbe()
    mediator watch aggregateRootActor
    mediator.send(aggregateRootActor, command)

    lastCommand = Some(command)

    mediator
      .expectMsgPF(duration, s"reply to command [$command]") {
        case Ko(x) =>
          lastFailure = Some(x)
        case Ok(events) =>
          handledEvents ++= events.toList
      }

    this
  }

  /**
    * Fetch the current state of the Aggregate Root Actor.
    * As the state is stored in the actor, the method will ask the actor for its' internal state so the method
    * returns a Future that will complete when the state is returned.
    *
    * @return Future with the AggregateState as defined for this Aggregate Root.
    */
  def currentState: Future[Option[B]] = (aggregateRootActor ? GetState).mapTo[Option[B]]

  private def assumingCommandIssued[T](thunk: DomainCommand => T): T =
    lastCommand.fold(throw NoCommandsIssued)(thunk)

  /** Events emitted by the aggregate root in reaction to command(s)
    */
  def events: List[DomainEvent] = assumingCommandIssued(_ => handledEvents)

  /** Last known command handling failure */
  def failure: HandlingFailure =
    assumingCommandIssued(c => lastFailure.getOrElse(throw UnexpectedCommandHandlingSuccess(c)))

  override def toString: String = {
    s"Aggregate Root ${ctag.runtimeClass.getSimpleName} ${id.idAsString}"
  }

}
