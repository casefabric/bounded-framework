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
package io.cafienne.bounded.cargosample.httpapi

import akka.actor._
import akka.stream.ActorMaterializer
import com.github.swagger.akka._
import com.github.swagger.akka.model.Info

class SwaggerHttpServiceRoute(val system: ActorSystem, val mat: ActorMaterializer) extends SwaggerHttpService {
  implicit val actorSystem: ActorSystem = system
  implicit val materializer: ActorMaterializer = mat

  override val apiClasses = Set(classOf[HttpApiEndpoint], classOf[CargoRoute])

  override val basePath = "/" //the basePath for the API you are exposing
  override val apiDocsPath = "api-docs" //where you want the swagger-json endpoint exposed

  override val info = Info(description =
    """Cargo sample services.
      |
      |Allows..
      |
      """.stripMargin, version = "1.0.0")

  def swaggerUIRoute = get {
    routes ~
      pathPrefix("") {
        pathEndOrSingleSlash {
          getFromResource("swagger-ui/index.html")
        }
      } ~ getFromResourceDirectory("swagger-ui")
  }

}
