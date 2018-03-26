/*
 * Copyright (C) 2016-2018 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.cargosample.projections

import akka.actor.ActorSystem
import io.cafienne.bounded.cargosample.domain.CargoDomainProtocol

import scala.concurrent.Future

class CargoQueriesImpl()(implicit val system: ActorSystem) extends CargoQueries {

  override def getCargo(cargoId: CargoDomainProtocol.CargoId): Future[QueriesJsonProtocol.CargoViewItem] = {
    CargoViewProjectionWriter.getCargo(cargoId)
  }

}
