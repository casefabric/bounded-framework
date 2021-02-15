/*
 * Copyright (C) 2016-2021 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.aggregate.typed

import akka.actor.typed.Behavior
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import io.cafienne.bounded.aggregate.DomainCommand

trait TypedAggregateRootManager[T <: DomainCommand] {
  def behavior(id: String): Behavior[T]

  /**
    * Used for Cluster Sharding
    * @tparam T Aggregate Root Type as defined by the Protocol.
    * @return EntityType used for sharding the Aggregate Root T
    */
  def entityTypeKey: EntityTypeKey[_]
}
