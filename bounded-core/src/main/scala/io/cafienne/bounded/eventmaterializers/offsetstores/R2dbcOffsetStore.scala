/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.eventmaterializers.offsetstores

import org.apache.pekko.Done
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.adapter._
import org.apache.pekko.persistence.query.{NoOffset, Offset}
import org.apache.pekko.persistence.r2dbc.ConnectionFactoryProvider
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import com.typesafe.config.Config
import io.r2dbc.spi.{ConnectionFactory, Result, Row, RowMetadata}
import org.reactivestreams.Publisher

import java.util.function.BiFunction

import scala.concurrent.{ExecutionContext, Future}

/**
  * Offset store for the R2DBC journal (Postgres or Yugabyte), in the table `bounded_offset_store`. It uses the
  * connection pool of the given connection factory config, by default the one of the journal.
  *
  * @param connectionFactoryPath config path of an R2DBC connection factory, e.g.
  *                              `pekko.persistence.r2dbc.connection-factory`
  */
class R2dbcOffsetStore(system: ActorSystem, connectionFactoryPath: String) extends OffsetStore {
  import R2dbcOffsetStore._

  private implicit val actorSystem: ActorSystem = system
  private implicit val ec: ExecutionContext     = system.dispatcher

  private val connectionFactory: ConnectionFactory =
    ConnectionFactoryProvider(system.toTyped).connectionFactoryFor(connectionFactoryPath)

  private val tableCreated = new CreateOnFirstUse(() => execute(CreateTable).map(_ => ()))

  override def saveOffset(viewIdentifier: String, offset: Offset): Future[Unit] = {
    val encoded = OffsetCodec.encode(offset)
    tableCreated()
      .flatMap(_ => execute(Upsert, viewIdentifier, encoded.offsetType, encoded.value))
      .map(_ => ())
  }

  override def getOffset(viewIdentifier: String): Future[Offset] =
    tableCreated()
      .flatMap(_ =>
        query(Select, viewIdentifier) { (row, _) =>
          OffsetCodec.decode(row.get("offset_type", classOf[String]), row.get("offset_value", classOf[String]))
        }
      )
      .map(_.headOption.getOrElse(NoOffset))

  override def clear(): Future[Done] =
    tableCreated().flatMap(_ => execute(DeleteAll)).map(_ => Done)

  override def clear(viewIdentifier: String): Future[Done] =
    tableCreated().flatMap(_ => execute(Delete, viewIdentifier)).map(_ => Done)

  private def execute(sql: String, binds: String*): Future[Seq[Long]] =
    run(sql, binds)(result => result.getRowsUpdated).map(_.map(_.longValue))

  private def query[T](sql: String, binds: String*)(read: (Row, RowMetadata) => T): Future[Seq[T]] =
    run(sql, binds)(result =>
      result.map(new BiFunction[Row, RowMetadata, T] {
        override def apply(row: Row, metadata: RowMetadata): T = read(row, metadata)
      })
    )

  // Runs one statement on a connection from the pool and closes the connection, also when the statement fails.
  private def run[T](sql: String, binds: Seq[String])(read: Result => Publisher[T]): Future[Seq[T]] =
    Source.fromPublisher(connectionFactory.create()).runWith(Sink.head).flatMap { connection =>
      val statement = binds.zipWithIndex.foldLeft(connection.createStatement(sql)) {
        case (stmt, (value, index)) => stmt.bind(index, value)
      }
      val results = Source
        .fromPublisher(statement.execute())
        .flatMapConcat(result => Source.fromPublisher(read(result)))
        .runWith(Sink.seq)
      results.transformWith { outcome =>
        Source.fromPublisher(connection.close()).runWith(Sink.ignore).transform(_ => outcome)
      }
    }
}

object R2dbcOffsetStore {

  val ConnectionFactoryConfigPath = "bounded.eventmaterializers.r2dbc-offsetstore.connection-factory"
  val JournalConnectionFactory    = "pekko.persistence.r2dbc.connection-factory"

  /**
    * The config path of the connection factory for the offset store: the one named at
    * `bounded.eventmaterializers.r2dbc-offsetstore.connection-factory`, else the one of the R2DBC journal.
    */
  def connectionFactoryPath(config: Config): String =
    if (config.hasPath(ConnectionFactoryConfigPath)) config.getString(ConnectionFactoryConfigPath)
    else JournalConnectionFactory

  val CreateTable: String =
    "CREATE TABLE IF NOT EXISTS bounded_offset_store (view_identifier VARCHAR(255) PRIMARY KEY, offset_type VARCHAR(32) NOT NULL, offset_value TEXT NOT NULL)"

  val Upsert: String =
    "INSERT INTO bounded_offset_store (view_identifier, offset_type, offset_value) VALUES ($1, $2, $3) " +
      "ON CONFLICT (view_identifier) DO UPDATE SET offset_type = excluded.offset_type, offset_value = excluded.offset_value"

  val Select: String = "SELECT offset_type, offset_value FROM bounded_offset_store WHERE view_identifier = $1"

  val Delete: String = "DELETE FROM bounded_offset_store WHERE view_identifier = $1"

  val DeleteAll: String = "DELETE FROM bounded_offset_store"
}
