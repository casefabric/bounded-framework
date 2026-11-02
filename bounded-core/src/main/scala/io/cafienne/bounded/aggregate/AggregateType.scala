/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.aggregate

import org.apache.pekko.persistence.typed.PersistenceId

/**
  * The type of an aggregate root, e.g. `AggregateType("order")`. Every aggregate has one: it is the first part of
  * the aggregate's persistence id (`order|<aggregate id>`) and every event the aggregate stores is tagged with it.
  * Event materializers and sagas select the events they read by aggregate type; Bounded reads them by tag or, on the
  * R2DBC journal, by entity type, so users never deal with either.
  *
  * Define it once per aggregate and use that value on both sides:
  * {{{
  * object Order { val Type: AggregateType = AggregateType("order") }
  * }}}
  *
  * The name is part of every stored persistence id, so it can never be changed without migrating the journal.
  *
  * @param name the name of the type: not empty and without `|`, the separator of the persistence id
  */
final case class AggregateType(name: String) {
  require(name.nonEmpty, "An aggregate type needs a name")
  require(
    !name.contains(PersistenceId.DefaultSeparator),
    s"Aggregate type [$name] must not contain '${PersistenceId.DefaultSeparator}', the separator of the persistence id"
  )

  /**
    * The persistence id of the aggregate with the given id: `name|aggregateId`.
    */
  def persistenceId(aggregateId: String): PersistenceId = PersistenceId(name, aggregateId)

  override def toString: String = name
}

object AggregateType {

  /**
    * The aggregate type of a persistence id written as `type|id`, None for any other persistence id.
    */
  def of(persistenceId: String): Option[AggregateType] = {
    val separator = persistenceId.indexOf(PersistenceId.DefaultSeparator)
    if (separator <= 0) None else Some(AggregateType(persistenceId.substring(0, separator)))
  }

  /**
    * The aggregate id in a persistence id written as `type|id`, None for any other persistence id.
    */
  def aggregateIdOf(persistenceId: String): Option[String] = {
    val separator = persistenceId.indexOf(PersistenceId.DefaultSeparator)
    if (separator <= 0) None else Some(persistenceId.substring(separator + 1))
  }
}
