/*
 * Copyright (C) 2016-2020 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.test

import java.time.{OffsetDateTime, ZonedDateTime}
import java.util.UUID

import io.cafienne.bounded.aggregate.{CommandMetaData, MetaData}
import io.cafienne.bounded.{BuildInfo, RuntimeInfo, UserContext}

case class TestCommandMetaData(
  timestamp: OffsetDateTime,
  val userContext: Option[UserContext],
  override val commandId: UUID = UUID.randomUUID()
) extends CommandMetaData

case class TestMetaData(
  timestamp: OffsetDateTime,
  userContext: Option[UserContext],
  causedByCommand: Option[UUID],
  buildInfo: BuildInfo,
  runTimeInfo: RuntimeInfo
) extends MetaData

object TestMetaData {
  def fromCommand(
    metadata: TestCommandMetaData
  )(implicit buildInfo: BuildInfo, runtimeInfo: RuntimeInfo): TestMetaData = {
    TestMetaData(
      metadata.timestamp,
      metadata.userContext,
      Some(metadata.commandId),
      buildInfo,
      runtimeInfo
    )
  }
}
