/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.projection

import org.apache.pekko.projection.ProjectionId
import io.cafienne.bounded.aggregate.{AggregateType, DomainEvent}
import io.cafienne.bounded.eventmaterializers.{MaterializerEventFilter, NoFilterEventFilter}

import java.util.UUID

/**
  * A Bounded read model run as a Pekko Projection: the events of one aggregate type, with an optional filter. Build
  * the projection with the factory of its store (`BoundedJdbcProjection`, `BoundedSlickProjection`,
  * `BoundedR2dbcProjection`, `BoundedCassandraProjection`) and run it with `BoundedProjections`.
  *
  * Unlike Bounded's event materializers, a projection keeps its offset in the offset store of Pekko Projections (in
  * the database of the projection style), not in a Bounded `OffsetStore`.
  */
trait EventProjection {

  /** The name of the projection. With the aggregate type it identifies the stored offset, so it must not change. */
  def projectionName: String

  /** The type of the aggregates whose events the projection handles. */
  def aggregateType: AggregateType

  /**
    * Events the filter rejects are not handled, but their offset is stored. The filter judges domain events only;
    * other events are handled.
    */
  def eventFilter: MaterializerEventFilter = NoFilterEventFilter

  /** Identifies this projection in the `EventProcessed` messages it publishes. */
  val materializerId: UUID = UUID.randomUUID()

  /** The id of the projection: its name, and the aggregate type as its key. */
  final def projectionId: ProjectionId = ProjectionId(projectionName, aggregateType.name)

  /**
    * The id under which the projection publishes its progress, and under which `ReadSideRegistry.awaitProcessed`
    * waits for it: the id of `projectionId`, `name-aggregatetype`. All slice ranges of an R2DBC projection share it.
    */
  final def readSideId: String = projectionId.id

  /** Whether the filter accepts the event. */
  private[projection] final def accepts(event: Any): Boolean = event match {
    case domainEvent: DomainEvent => eventFilter.filter(domainEvent)
    case _                        => true
  }
}
