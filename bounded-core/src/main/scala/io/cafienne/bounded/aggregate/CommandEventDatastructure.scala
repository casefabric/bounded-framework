/*
 * Copyright (C) 2016-2018 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.aggregate

import java.util.UUID

trait Id {
  def idAsString: String
}

trait AggregateState {
  def update(event: DomainEvent): AggregateState
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
