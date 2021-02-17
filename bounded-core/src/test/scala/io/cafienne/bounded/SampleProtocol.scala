/*
 * Copyright (C) 2016-2021 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded

import java.time.OffsetDateTime
import java.util.UUID

object SampleProtocol {

  trait Id {
    def idAsString: String
  }

  trait UserId extends Id

  trait UserContext {
    def userId: UserId

    def roles: List[String]
  }

  /**
    * Metadata of the event contains data that is used within the framework and may be used by the application
    * @param timestamp the moment the event was created
    * @param userContext contains an assumed known user, events generated via an HTTP API will most of the time be authenticated
    */
  trait CommandMetaData {
    def timestamp: OffsetDateTime
    def userContext: Option[UserContext]
    val commandId: UUID = UUID.randomUUID()
  }

  /**
    * Metadata of the event contains data that is used within the framework and may be used by the application
    * @param timestamp the moment the event was created
    * @param userContext contains an assumed known user, events generated via an HTTP API will most of the time be authenticated
    * @param causedByCommand contains a reference to the command that has caused this event.
    * @param buildInfo contains the build information of the application. The Version is used to ensure version specific routing of messages.
    * @param runTimeInfo contains information on the runtime the event is generated and stored
    */
  trait MetaData {
    def timestamp: OffsetDateTime
    def userContext: Option[UserContext]
    def causedByCommand: Option[UUID]
  }

}
