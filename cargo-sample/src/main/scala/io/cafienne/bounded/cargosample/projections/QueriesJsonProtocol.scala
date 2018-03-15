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
package io.cafienne.bounded.cargosample.projections

import io.cafienne.bounded.cargosample.domain.CargoDomainProtocol.CargoId
import io.swagger.annotations.{ApiModel, ApiModelProperty}
import java.time.ZonedDateTime
import scala.annotation.meta.field
import spray.json._

object QueriesJsonProtocol extends DefaultJsonProtocol {
  import io.cafienne.bounded.cargosample.domain.CargoDomainJsonProtocol._
  import io.cafienne.bounded.aggregate.CommandEventDatastructureJsonProtocol._


  @ApiModel(description = "Details of a single Cargo")
  case class CargoViewItem(
    @(ApiModelProperty @field)(value = "Unique identifier of thie Cargo", required = true, dataType = "string") id: CargoId,
    @(ApiModelProperty @field)(value = "origin", required = true, dataType = "string") origin: String,
    @(ApiModelProperty @field)(value = "destination", required = true, dataType = "string") destination: String,
    @(ApiModelProperty @field)(value = "delivery due date", required = true, dataType = "string") deliveryDueDate: ZonedDateTime
  )

  implicit val cargoViewItemFmt = jsonFormat4(CargoViewItem)

}
