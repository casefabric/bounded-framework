/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded

import com.typesafe.config.{Config, ConfigFactory}
import org.testcontainers.postgresql.PostgreSQLContainer

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import scala.io.Source
import scala.util.Using

/**
  * Postgres in a container with the tables of the R2DBC journal, and the config to run an actor system on it. Only
  * start it when Docker is available (see `DockerTests`).
  */
final class R2dbcPostgres {
  private val container = new PostgreSQLContainer("postgres:16-alpine")
  container.start()

  private val ddl = Using.resource(Source.fromResource("r2dbc/create_tables_postgres.sql"))(_.mkString)
  ddl.split(";").map(_.trim).filter(_.nonEmpty).foreach(statement => update(statement))

  /**
    * The R2DBC journal and snapshot store on this database, with short polling and backtracking intervals so that
    * tests do not wait long for live and late events.
    */
  val config: Config = ConfigFactory
    .parseString(s"""
      pekko.loglevel = INFO
      pekko.stdout-loglevel = INFO
      pekko.persistence.journal.plugin = "pekko.persistence.r2dbc.journal"
      pekko.persistence.snapshot-store.plugin = "pekko.persistence.r2dbc.snapshot"
      pekko.persistence.r2dbc.connection-factory {
        host = "${container.getHost}"
        port = ${container.getMappedPort(5432)}
        database = "${container.getDatabaseName}"
        user = "${container.getUsername}"
        password = "${container.getPassword}"
      }
      pekko.persistence.r2dbc.query {
        refresh-interval = 200ms
        backtracking.window = 1m
        backtracking.behind-current-time = 1s
      }
    """)
    .withFallback(ConfigFactory.load())

  def update(sql: String, binds: Any*): Int =
    withStatement(sql, binds)(_.executeUpdate())

  def query[T](sql: String, binds: Any*)(read: ResultSet => T): List[T] =
    withStatement(sql, binds) { statement =>
      Using.resource(statement.executeQuery()) { rows =>
        Iterator.continually(rows).takeWhile(_.next()).map(read).toList
      }
    }

  def stop(): Unit = container.stop()

  private def withStatement[T](sql: String, binds: Seq[Any])(run: PreparedStatement => T): T =
    Using.resource(connection()) { connection =>
      Using.resource(connection.prepareStatement(sql)) { statement =>
        binds.zipWithIndex.foreach { case (value, index) => statement.setObject(index + 1, value) }
        run(statement)
      }
    }

  private def connection(): Connection =
    DriverManager.getConnection(container.getJdbcUrl, container.getUsername, container.getPassword)
}
