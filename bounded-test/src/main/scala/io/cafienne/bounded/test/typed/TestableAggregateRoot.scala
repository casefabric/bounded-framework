/*
 * Copyright (C) 2016-2022 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.test.typed

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorSystem
import akka.actor.testkit.typed.scaladsl.ActorTestKit
import akka.actor.typed.eventstream.EventStream.Subscribe
import akka.actor.typed.{Behavior, ChildFailed, SupervisorStrategy}
import akka.actor.typed.scaladsl.Behaviors
import io.cafienne.bounded.test.typed.TestableAggregateRoot._

import scala.reflect.ClassTag
import akka.util.Timeout
import io.cafienne.bounded.aggregate.{DomainCommand, DomainEvent, HandlingFailure}
import io.cafienne.bounded.aggregate.typed._

import scala.concurrent.duration.Duration
import akka.actor.typed.scaladsl.adapter._
import akka.persistence.testkit.scaladsl.PersistenceTestKit

import scala.collection.immutable

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

  case class AggregateThrownException(ex: Throwable) extends HandlingFailure

  case class CommandHandlingFailed(failure: HandlingFailure)
      extends Exception(s"Command handling failed with [$failure]")

  case class MisdirectedCommand(expectedId: String, command: DomainCommand)
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
  def given[A <: DomainCommand, B <: DomainEvent, C: ClassTag](
    creator: TypedAggregateRootManager[A],
    id: String,
    evt: B*
  )(implicit system: ActorSystem, timeout: Timeout, ctag: reflect.ClassTag[A]): TestableAggregateRoot[A, B, C] = {
    new TestableAggregateRoot[A, B, C](creator, id, immutable.Seq.concat(evt))
  }

  /** Construct a test for a specific aggregate root that hos no initial state. This can be used to test creation of the
    * aggregate root and see if it is constructed with the right initial state.
    *
    * @param id The aggregate root ID that is used for testing
    * @tparam A The Aggregate Root Type that is tested.
    * @return a TestableAggregateRoot instance that is initialized and available to give a DomainCommand to.
    */
  def given[A <: DomainCommand, B <: DomainEvent, C: ClassTag](
    creator: TypedAggregateRootManager[A],
    id: String
  )(implicit system: ActorSystem, timeout: Timeout, ctag: reflect.ClassTag[A]): TestableAggregateRoot[A, B, C] = {
    new TestableAggregateRoot[A, B, C](creator, id, immutable.Seq.empty[B])
  }

  //The tested aggregate root makes use of an additional counter in the id in order to prevent collision of parallel running tests.
  private val atomicCounter: AtomicInteger = new AtomicInteger()
  private def testId(id: String): String   = id + "-" + atomicCounter.getAndIncrement().toString

}

/**
  *
  * @param manager Typed aggregate root manager handles the lifecycle of the Aggregate Root.
  *       @see io.cafienne.bounded.aggregate.typed.TypedAggregateRootManager for more details
  * @param id The aggregate root ID that is used for testing
  * @param evt Seqence of events that need to be in the eventstore *before* the aggregate is started. Will trigger replay of the state for these events.
  * @param persistenceIdSeparator Typed actors use a persistenceId that contains the typehint and the id of the actor seperated by a
  *                               seperator. Default is use in Lagom, scaladsl.PersistentEntity
  * @param system actor system to use for the tests
  * @param timeout call timeout
  * @param ctag Classtag
  * @tparam A DomainCommand based commands
  * @tparam B DomainEvent based events
  * @tparam C ClassTag
  */
class TestableAggregateRoot[A <: DomainCommand, B <: DomainEvent, C: ClassTag] private (
  manager: TypedAggregateRootManager[A],
  id: String,
  evt: immutable.Seq[B],
  persistenceIdSeparator: String = "|"
)(
  implicit system: ActorSystem,
  timeout: Timeout,
  ctag: reflect.ClassTag[A]
) {

  implicit val typedActorSystem    = system.toTyped
  val testKit                      = ActorTestKit(system.toTyped)
  val persistenceTestKit           = PersistenceTestKit(system)
  implicit val duration: Duration  = timeout.duration
  private val givenEvents: List[B] = evt.toList

  private var lastFailure: Option[HandlingFailure] = Option.empty
  private var lastCommand: Option[DomainCommand]   = Option.empty

  import TestableAggregateRoot.testId
  private final val arTestId = testId(id)
  if (evt != null && evt.nonEmpty) persistenceTestKit.persistForRecovery(arTestId, evt)

  def supervise: Behavior[A] =
    Behaviors.supervise(handler).onFailure(SupervisorStrategy.stop)

  def handler: Behavior[A] = Behaviors.setup { context =>
    val aggregateRootActor = context.spawn(manager.behavior(arTestId), s"test-aggregate-$arTestId")
    context.watch(aggregateRootActor)
    Behaviors
      .receiveMessage[A] { message =>
        aggregateRootActor ! message
        Behaviors.same
      }
      .receiveSignal(
        {
          case (context, ChildFailed(ref, sig)) =>
            context.log.debug("behavior Failed " + sig.getMessage)
            lastFailure = Some(AggregateThrownException(sig))
            Behaviors.same
          case other =>
            context.log.debug("Received signal " + other)
            Behaviors.same
        }
      )
  }

  private val wrappedActor = testKit.spawn(supervise, s"supervisor-$arTestId")
  private val eventProbe   = testKit.createTestProbe[Any]("EventStreamProbe")
  testKit.internalSystem.eventStream.tell(Subscribe(eventProbe.ref))

  /**
    * After initialization of the aggregate root, the when allows to send a command and check thereafter what events are
    * created.
    *
    * @param command The DomainCommand that the aggregate root needs to process.
    * @return This initialized TestableAggregateRoot that processed the command.
    */
  def when(command: A): TestableAggregateRoot[A, B, C] = {
    if (command.aggregateRootId != id) throw MisdirectedCommand(id, command)
    wrappedActor.tell(command)
    lastCommand = Some(command)
    Thread.sleep(1000)
    this
  }

  private def assumingCommandIssued[T](thunk: DomainCommand => T): T =
    lastCommand.fold(throw NoCommandsIssued)(thunk)

  /** Events emitted by the aggregate root in reaction to command(s)
    */
  def events: List[DomainEvent] = {
    if (lastCommand.isEmpty) throw NoCommandsIssued
    val unwanted = givenEvents.toSet
    persistenceTestKit.persistedInStorage(arTestId).map(_.asInstanceOf[B]).toList.filterNot(unwanted)
  }

  /** Last known command handling failure */
  def failure: HandlingFailure =
    assumingCommandIssued(c => lastFailure.getOrElse(throw UnexpectedCommandHandlingSuccess(c)))

  override def toString: String = {
    s"Aggregate Root ${ctag.runtimeClass.getSimpleName} $id"
  }

  def shutdown: Unit = {
    testKit.shutdownTestKit()
  }

}
