/*
 * Copyright (C) 2016-2019 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

import sbt._

object Dependencies {

  val akkaVersion = "2.6.21"
  val akkaHttpVersion = "10.2.9"
  val staminaVersion = "0.1.6"
  val persistenceInMemVersion = "2.5.15.2"
  val scalaTestVersion = "3.2.11"

  val baseDeps = {
    def akkaModule(name: String, version: String = akkaVersion) =
      "com.typesafe.akka" %% s"akka-$name" % version
    Seq(
      akkaModule("slf4j"),
      akkaModule("actor"),
      akkaModule("stream"),
      akkaModule("persistence"),
      akkaModule("persistence-typed"),
      akkaModule("persistence-query"),
      akkaModule("cluster-sharding-typed"),
      akkaModule("cluster"),
      akkaModule("coordination"),
      akkaModule("cluster-tools"),
      akkaModule("stream-testkit") % Test,
      akkaModule("testkit") % Test,
      akkaModule("actor-testkit-typed") % Test,
      akkaModule("persistence-testkit") % Test,
      "io.spray"                    %% "spray-json"                             % "1.3.6",
      "com.github.dnvriend"         %% "akka-persistence-inmemory"              % persistenceInMemVersion,
      "com.typesafe.scala-logging"  %% "scala-logging"                          % "3.9.5"
    )
  }

  val log = Seq(
      "ch.qos.logback" % "logback-classic" % "1.4.7",
      "net.logstash.logback" % "logstash-logback-encoder" % "7.3"
    )


  val  test = log ++ Seq(
      "org.scalatest"       %% "scalatest"                   % scalaTestVersion % Test
    )


  val akkaHttpDeps = {
    def akkaHttpModule(name: String, version: String = akkaHttpVersion) =
      "com.typesafe.akka" %% s"akka-$name" % version

    baseDeps ++ Seq(
      akkaHttpModule("http"),
      akkaHttpModule("http-spray-json"),
      akkaHttpModule("http-testkit") % Test
    ) ++ test
  }

  val testDeps = {
    baseDeps ++ Seq(
      "org.scalatest"          %% "scalatest"                 % scalaTestVersion,
      "com.typesafe.akka"      %% "akka-testkit"              % akkaVersion,
      "com.typesafe.akka"      %% "akka-actor-testkit-typed"  % akkaVersion,
      "com.typesafe.akka"      %% "akka-persistence-testkit"  % akkaVersion,
      "com.typesafe.akka"      %% "akka-http-testkit"         % akkaHttpVersion
    ) ++ test
  }

  val persistenceLevelDBDeps = {
    baseDeps ++ Seq(
      "org.iq80.leveldb"            % "leveldb"        % "0.12",
      "org.fusesource.leveldbjni"   % "leveldbjni-all" % "1.8"
    )
  }

  val persistanceLmdbDBDeps = {
    baseDeps ++ Seq(
      "org.lmdbjava"                % "lmdbjava"        % "0.8.2"
    )
  }

  val persistenceCassandraDeps = {
    baseDeps ++ Seq(
      "com.typesafe.akka" %% "akka-persistence-cassandra" % "1.0.5"
    )
  }

  val persistenceJdbcDeps = {
    baseDeps ++ Seq(
      "com.lightbend.akka" %% "akka-persistence-jdbc" % "5.0.4",
      "com.typesafe.slick" %% "slick" % "3.3.3"
    )
  }

}
