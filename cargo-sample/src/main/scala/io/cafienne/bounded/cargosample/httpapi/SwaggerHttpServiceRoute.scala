/*
 * Copyright (C) 2016-2018 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

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
