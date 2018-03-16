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
package io.cafienne.bounded.cargosample.persistence

import io.cafienne.bounded.akka.persistence.persisters.ForwardsCompatibleSerializer
import io.cafienne.bounded.cargosample.domain.CargoDomainProtocol.{CargoPlanned, NewRouteSpecified}
import stamina.json.persister

object CargoPersisters {

  import io.cafienne.bounded.cargosample.persistence.CargoDomainEventJsonProtocol._

  val v1CargoPlanned = persister[CargoPlanned]("cargoplanned")
  val v1NewRouteSpecified = persister[NewRouteSpecified]("newroutespecified")

  def persisters = List(
    v1CargoPlanned,
    v1NewRouteSpecified
  )
}

class CargoPersistersSerializer extends ForwardsCompatibleSerializer(CargoPersisters.persisters) {}

