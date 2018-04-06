/*
 * Copyright (C) 2016-2018 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.cargosample.domain

import io.cafienne.bounded.cargosample.domain.CargoDomainProtocol.Location

/**
  * The LocationsProvider is a sample service that is used as a depencency in the aggregate root.
  * This is a very simple sample to show you you can inject dependencies into the Aggregate Root that is
  * created.
  */
trait LocationsProvider {

  def getLocation(input: String): Option[Location]

}

class FixedLocationsProvider extends LocationsProvider {

  val locations = Map("Oslo" -> Location("Oslo"), "Amsterdam" -> Location("Amsterdam"))

  def getLocation(input: String) = locations.get(input)

}

object FixedLocationsProvider {

  private val fixedLocationsProvider = new FixedLocationsProvider()

  def apply(): LocationsProvider = fixedLocationsProvider

}
