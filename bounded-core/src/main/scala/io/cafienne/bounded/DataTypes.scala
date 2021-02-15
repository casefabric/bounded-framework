/*
 * Copyright (C) 2016-2021 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded

trait Id {
  def idAsString: String
}

trait UserId extends Id

trait UserContext {
  def userId: UserId

  def roles: List[String]
}

case class BuildInfo(name: String, version: String)

case class RuntimeInfo(id: String)
