/*
 * Copyright (C) 2016-2018 Cafienne B.V. <https://www.cafienne.io/bounded>
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

/**
  * Version number of the build info is based on the Apache Runtime Versioning rules:
  * @see https://apr.apache.org/versioning.html
  * @param major
  * @param minor
  * @param patch
  */
case class Version(major: Integer, minor: Integer, patch: Integer)

case class BuildInfo(name: String, version: String)

case class RuntimeInfo(id: String)

object RuntimeCompatibility extends Enumeration {
  type RuntimeCompatibility = Value
  val ALL, CURRENT = Value
}

object VersionCompatibility extends Enumeration {
  type VersionCompatibility = Value
  val ALL, CURRENT = Value
}

case class Compatibility(
  runtime: RuntimeCompatibility.RuntimeCompatibility,
  version: VersionCompatibility.VersionCompatibility
)

object DefaultCompatibility extends Compatibility(RuntimeCompatibility.ALL, VersionCompatibility.ALL)
