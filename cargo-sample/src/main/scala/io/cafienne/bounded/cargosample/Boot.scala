/*
 * Copyright (C) 2016-2018 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.cargosample

import java.io.File

import akka.actor.ActorSystem
import akka.event.{Logging, LoggingAdapter}
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import akka.util.Timeout
import io.cafienne.bounded.aggregate.DefaultCommandGateway
import io.cafienne.bounded.eventmaterializers._
import io.cafienne.bounded.cargosample.domain.{CargoCreator, FixedLocationsProvider}
import io.cafienne.bounded.cargosample.httpapi.HttpApiEndpoint
import io.cafienne.bounded.cargosample.projections.{CargoLmdbClient, CargoQueriesImpl, CargoViewProjectionWriter}
import io.cafienne.bounded.config.Configured

import scala.concurrent.Await
import scala.util.{Failure, Success}

object Boot extends App with Configured {
  implicit val system       = ActorSystem("cargo-service")
  implicit val materializer = ActorMaterializer()
  implicit val http         = Http()

  import system.dispatcher
  import scala.concurrent.duration._

  implicit val logger: LoggingAdapter = Logging(system, getClass)

  val host = config.getString("application.bind.host")
  val port = config.getInt("application.bind.port")

  sys addShutdownHook {
    logger.info("Shutting down the Cargo Service")
    Await.ready(system.terminate(), 20.seconds).onComplete {
      case Success(s)   => logger.info(s"system terminated with $s")
      case Failure(msg) => logger.error(s"system shutdown failed due to $msg")
    }
  }

  val lmdbPath        = config.getString("application.lmdb-path")
  val cargoLmdbClient = new CargoLmdbClient(new File(lmdbPath, "cargo"))

  val cargoQueries              = new CargoQueriesImpl(cargoLmdbClient)
  val cargoViewProjectionWriter = new CargoViewProjectionWriter(system, cargoLmdbClient) with ReadJournalOffsetStore

  val eventMaterializers = new EventMaterializers(
    List(
      cargoViewProjectionWriter
    )
  )

  implicit val timeout = Timeout(5.seconds)
  val commandGateway   = new DefaultCommandGateway(system, new CargoCreator(system, new FixedLocationsProvider()))

  val httpApiEndpoint = new HttpApiEndpoint(
    commandGateway,
    cargoQueries
  )

  try {
    // Startup event listeners. After replay start http server
    eventMaterializers.startUp(true).onComplete {
      case Success(msg) =>
        logger.info(s"Start HTTP server")
        httpApiEndpoint.runServer(host, port)
      case Failure(msg) =>
        logger.error(
          "Could not start Cargo Service due to {}",
          msg.getMessage + Option(msg.getCause).fold("")(t => s" Cause: ${t.getMessage} ")
        )
    }
  } catch {
    case t: Throwable =>
      logger.error(s"Boot loop crashed with ${t.getMessage}, stacktrace: ")
      t.printStackTrace()
  }

}
