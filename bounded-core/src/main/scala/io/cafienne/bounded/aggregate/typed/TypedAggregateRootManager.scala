/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.aggregate.typed

import org.apache.pekko.actor.typed.Behavior
import org.apache.pekko.cluster.sharding.typed.scaladsl.EntityTypeKey
import org.apache.pekko.persistence.typed.PersistenceId
import io.cafienne.bounded.aggregate.{AggregateType, DomainCommand}

trait TypedAggregateRootManager[T <: DomainCommand] {

  /**
    * The behavior of the aggregate with the given persistence id. The gateways call it with
    * `aggregateType.persistenceId(aggregateRootId)`; the aggregate id is `AggregateType.aggregateIdOf(persistenceId)`.
    *
    * The behavior must persist under exactly this persistence id and tag every event with the aggregate type, which
    * is how event materializers find its events. `AggregateBehavior` builds an `EventSourcedBehavior` that does both;
    * a behavior built by hand uses `EventSourcedBehavior(persistenceId, ...).withTagger(_ => Set(aggregateType.name))`.
    *
    * @param persistenceId the persistence id of the aggregate, `aggregateType|aggregateRootId`
    */
  def behavior(persistenceId: PersistenceId): Behavior[T]

  /**
    * Used for Cluster Sharding
    * @tparam T Aggregate Root Type as defined by the Protocol.
    * @return EntityType used for sharding the Aggregate Root T
    */
  def entityTypeKey: EntityTypeKey[_]

  /**
    * The type of the aggregate, by default the name of the `entityTypeKey`. It is part of every stored persistence id,
    * so it can never be changed without migrating the journal (see `AggregateTypeMigration`).
    */
  def aggregateType: AggregateType = AggregateType(entityTypeKey.name)

  /**
    * The persistence id of the aggregate with the given id: `aggregateType|aggregateRootId`.
    */
  final def persistenceId(aggregateRootId: String): PersistenceId = aggregateType.persistenceId(aggregateRootId)

  /**
    * The behavior of the aggregate with the given id, under its persistence id.
    */
  final def behaviorFor(aggregateRootId: String): Behavior[T] = behavior(persistenceId(aggregateRootId))
}
