/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.projection

import org.apache.pekko.Done
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.persistence.query.Offset
import org.apache.pekko.projection.ProjectionId
import org.apache.pekko.projection.eventsourced.scaladsl.EventSourcedProvider
import org.apache.pekko.projection.r2dbc.R2dbcProjectionSettings
import org.apache.pekko.projection.r2dbc.scaladsl.{R2dbcHandler, R2dbcProjection, R2dbcSession}
import org.apache.pekko.projection.scaladsl.{
  AtLeastOnceProjection,
  ExactlyOnceProjection,
  GroupedProjection,
  SourceProvider
}

import scala.concurrent.Future

/** A range of slices of the R2DBC journal, from `min` to `max` inclusive. */
final case class SliceRange(min: Int, max: Int)

/**
  * A Bounded read model as an `R2dbcProjection` (needs `pekko-projection-r2dbc`), with its offsets in the R2DBC
  * offset store (`pekko.projection.r2dbc`). Each method calls the `R2dbcProjection` method of the same name. It reads
  * the events of the aggregate type by slices on the R2DBC journal, and by tag on the other journals, so the envelope
  * type depends on the journal: the projection is typed with `?`.
  *
  * On the R2DBC journal a projection reads all slices, or the `slices` given: split a busy read model with
  * `sliceRanges` into projections that `BoundedProjections.runInCluster` spreads over the nodes. The projection id of
  * a range has the key `<aggregate type>-<min>-<max>`.
  *
  * The handler function gets the events; events the filter of the `EventProjection` rejects skip it, and their offset
  * is stored. The projection publishes `EventProcessed`, and is then given to `adjust`, which can change it with the
  * methods of the Pekko projection (`withRecoveryStrategy`, `withRestartBackoff`, `withSaveOffset`, `withGroup`,
  * ...). A `withStatusObserver` in `adjust` replaces the `EventProcessed` observer.
  */
object BoundedR2dbcProjection {

  /**
    * The slices of the R2DBC journal in `numberOfRanges` ranges, for one projection per range:
    * {{{
    * BoundedR2dbcProjection.sliceRanges(system, 4).map(range => BoundedR2dbcProjection.exactlyOnce(system, view, slices = Some(range))(handle))
    * }}}
    */
  def sliceRanges(system: ActorSystem[?], numberOfRanges: Int): Seq[SliceRange] = {
    require(BoundedProjections.isR2dbcJournal(system), "Slice ranges are only for the R2DBC journal")
    EventSourcedProvider
      .sliceRanges(system, BoundedProjections.queryPluginId(system), numberOfRanges)
      .map(range => SliceRange(range.min, range.max))
  }

  /** The statements `handle` runs on the session are in the transaction that stores the offset. */
  def exactlyOnce(
    system: ActorSystem[?],
    projection: EventProjection,
    settings: Option[R2dbcProjectionSettings] = None,
    slices: Option[SliceRange] = None,
    adjust: ExactlyOnceProjection[Offset, ?] => ExactlyOnceProjection[Offset, ?] = identity
  )(handle: (R2dbcSession, Any) => Future[Done]): ExactlyOnceProjection[Offset, ?] =
    withSource(system, projection, slices)([Envelope] =>
      (id: ProjectionId, source: SourceProvider[Offset, Envelope], envelope: ProjectionEnvelope[Envelope]) =>
        adjust(
          R2dbcProjection
            .exactlyOnce(id, settings, source, () => handler(projection)(handle)(using envelope))(system)
            .withStatusObserver(BoundedProjections.publishEventProcessed(system, projection)(using envelope))
        )
    )

  /**
    * The statements `handle` runs on the session are in a transaction of their own; the offsets are stored after the
    * events, in batches, so `handle` must be idempotent.
    */
  def atLeastOnce(
    system: ActorSystem[?],
    projection: EventProjection,
    settings: Option[R2dbcProjectionSettings] = None,
    slices: Option[SliceRange] = None,
    adjust: AtLeastOnceProjection[Offset, ?] => AtLeastOnceProjection[Offset, ?] = identity
  )(handle: (R2dbcSession, Any) => Future[Done]): AtLeastOnceProjection[Offset, ?] =
    withSource(system, projection, slices)([Envelope] =>
      (id: ProjectionId, source: SourceProvider[Offset, Envelope], envelope: ProjectionEnvelope[Envelope]) =>
        adjust(
          R2dbcProjection
            .atLeastOnce(id, settings, source, () => handler(projection)(handle)(using envelope))(system)
            .withStatusObserver(BoundedProjections.publishEventProcessed(system, projection)(using envelope))
        )
    )

  /** The offsets are stored after the events, in batches; `handle` must be idempotent. */
  def atLeastOnceAsync(
    system: ActorSystem[?],
    projection: EventProjection,
    settings: Option[R2dbcProjectionSettings] = None,
    slices: Option[SliceRange] = None,
    adjust: AtLeastOnceProjection[Offset, ?] => AtLeastOnceProjection[Offset, ?] = identity
  )(handle: Any => Future[Done]): AtLeastOnceProjection[Offset, ?] =
    withSource(system, projection, slices)([Envelope] =>
      (id: ProjectionId, source: SourceProvider[Offset, Envelope], envelope: ProjectionEnvelope[Envelope]) =>
        adjust(
          R2dbcProjection
            .atLeastOnceAsync(id, settings, source, () => BoundedHandlers.async(projection)(handle)(using envelope))(
              system
            )
            .withStatusObserver(BoundedProjections.publishEventProcessed(system, projection)(using envelope))
        )
    )

  /**
    * `handle` gets the events in groups (size and time set with `withGroup`); the statements it runs on the session
    * are in the transaction that stores the offset of the group.
    */
  def groupedWithin(
    system: ActorSystem[?],
    projection: EventProjection,
    settings: Option[R2dbcProjectionSettings] = None,
    slices: Option[SliceRange] = None,
    adjust: GroupedProjection[Offset, ?] => GroupedProjection[Offset, ?] = identity
  )(handle: (R2dbcSession, Seq[Any]) => Future[Done]): GroupedProjection[Offset, ?] =
    withSource(system, projection, slices)([Envelope] =>
      (id: ProjectionId, source: SourceProvider[Offset, Envelope], envelope: ProjectionEnvelope[Envelope]) =>
        adjust(
          R2dbcProjection
            .groupedWithin(id, settings, source, () => groupedHandler(projection)(handle)(using envelope))(system)
            .withStatusObserver(BoundedProjections.publishEventProcessed(system, projection)(using envelope))
        )
    )

  /** `handle` gets the events in groups; the offset of a group is stored after it, so `handle` must be idempotent. */
  def groupedWithinAsync(
    system: ActorSystem[?],
    projection: EventProjection,
    settings: Option[R2dbcProjectionSettings] = None,
    slices: Option[SliceRange] = None,
    adjust: GroupedProjection[Offset, ?] => GroupedProjection[Offset, ?] = identity
  )(handle: Seq[Any] => Future[Done]): GroupedProjection[Offset, ?] =
    withSource(system, projection, slices)([Envelope] =>
      (id: ProjectionId, source: SourceProvider[Offset, Envelope], envelope: ProjectionEnvelope[Envelope]) =>
        adjust(
          R2dbcProjection
            .groupedWithinAsync(
              id,
              settings,
              source,
              () => BoundedHandlers.groupedAsync(projection)(handle)(using envelope)
            )(system)
            .withStatusObserver(BoundedProjections.publishEventProcessed(system, projection)(using envelope))
        )
    )

  /** Builds the projection on the source for the journal: by slices on R2DBC, by tag on the others. */
  private def withSource[Result](system: ActorSystem[?], projection: EventProjection, slices: Option[SliceRange])(
    build: [Envelope] => (ProjectionId, SourceProvider[Offset, Envelope], ProjectionEnvelope[Envelope]) => Result
  ): Result =
    if (BoundedProjections.isR2dbcJournal(system)) {
      val range = slices.getOrElse(sliceRanges(system, 1).head)
      val id    = slices.fold(projection.projectionId)(range =>
        ProjectionId(projection.projectionName, s"${projection.aggregateType.name}-${range.min}-${range.max}")
      )
      build(id, BoundedProjections.eventsBySlices(system, projection, range), ProjectionEnvelope.bySlices[Any])
    } else {
      require(slices.isEmpty, "Slice ranges are only for the R2DBC journal")
      build(
        projection.projectionId,
        BoundedProjections.eventsByTag(system, projection, "R2dbcProjection"),
        ProjectionEnvelope.byTag[Any]
      )
    }

  private def handler[Envelope](projection: EventProjection)(handle: (R2dbcSession, Any) => Future[Done])(
    implicit envelope: ProjectionEnvelope[Envelope]
  ): R2dbcHandler[Envelope] = new R2dbcHandler[Envelope] {
    override def process(session: R2dbcSession, env: Envelope): Future[Done] = {
      val event = envelope.event(env)
      if (projection.accepts(event)) handle(session, event) else Future.successful(Done)
    }
  }

  private def groupedHandler[Envelope](projection: EventProjection)(
    handle: (R2dbcSession, Seq[Any]) => Future[Done]
  )(implicit envelope: ProjectionEnvelope[Envelope]): R2dbcHandler[Seq[Envelope]] = new R2dbcHandler[Seq[Envelope]] {
    override def process(session: R2dbcSession, envelopes: Seq[Envelope]): Future[Done] = {
      val events = BoundedHandlers.accepted(projection, envelopes)
      if (events.nonEmpty) handle(session, events) else Future.successful(Done)
    }
  }
}
