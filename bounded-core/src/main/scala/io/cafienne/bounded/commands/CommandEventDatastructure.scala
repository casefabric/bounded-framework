// Copyright (C) 2018 the original author or authors.
// See the LICENSE file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package io.cafienne.bounded.commands

import java.time.ZonedDateTime
import java.util.UUID
import io.cafienne.bounded.akka.persistence.eventmaterializers.EventNumber
import stamina.Persistable

trait Id {
  def idAsString: String
}

trait AggregateRootId extends Id

case class UserId(id: UUID) extends AggregateRootId {
  override def idAsString: String = id.toString

  override def toString: String = id.toString
}

trait UserContext {
  def userId: UserId

  def roles: List[String]

  private def canEqual(a: Any) = a.isInstanceOf[UserContext]

  override def equals(that: Any): Boolean =
    that match {
      case that: UserContext =>
        that.canEqual(this) && this.hashCode == that.hashCode
      case _ => false
    }

  override def hashCode: Int = {
    val prime = 31
    var result = 1
    result = prime * result + userId.idAsString.hashCode
    result = prime * result + (if (roles == null) 0 else roles.hashCode)

    result
  }
}

case class MetaData(timestamp: ZonedDateTime,
                    userContext: Option[UserContext],
                    eventId: Option[EventNumber])

trait AggregateRootCommand {
  def id: AggregateRootId

  def metaData: MetaData
}

trait AggregateRootEvent extends Persistable {
  def id: AggregateRootId

  def metaData: MetaData
}
