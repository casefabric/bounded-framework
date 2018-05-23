/*
 * Copyright (C) 2016-2018 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.cargosample.projections

import io.cafienne.bounded.cargosample.domain.CargoDomainProtocol.CargoId

import scala.concurrent.Future

trait CargoQueries {
  import QueriesJsonProtocol._

  def getCargo(cargoId: CargoId): Future[Option[CargoViewItem]]

}
