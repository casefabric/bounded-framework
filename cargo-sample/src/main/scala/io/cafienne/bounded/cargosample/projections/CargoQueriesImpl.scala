/*
 * Copyright (C) 2016-2018 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.cargosample.projections

import akka.actor.ActorSystem
import io.cafienne.bounded.cargosample.domain.CargoDomainProtocol

import scala.concurrent.Future

class CargoQueriesImpl(lmdbClient: LmdbClient)(implicit val system: ActorSystem) extends CargoQueries {

  import spray.json._
  import QueriesJsonProtocol.cargoViewItemFmt

  override def getCargo(cargoId: CargoDomainProtocol.CargoId): Future[Option[QueriesJsonProtocol.CargoViewItem]] = {
    val result = lmdbClient.get(cargoId.idAsString).map { value =>
      JsonParser(value).convertTo[QueriesJsonProtocol.CargoViewItem]
    }
    Future.successful(result)
  }

}
