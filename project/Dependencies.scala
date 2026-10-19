/*
 * Copyright (C) 2016-2019 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

import sbt._

object Dependencies {

  val pekkoVersion = "1.7.0"
  val pekkoHttpVersion = "1.4.0"
  val pekkoProjectionSnapshotVersion = "1.1.0"
  val scalaTestVersion = "3.2.20"
  val slickVersion = "3.6.1"
  val nettyVersion = "4.1.137.Final"

  val baseDeps = {
    def pekkoModule(name: String, version: String = pekkoVersion) =
      "org.apache.pekko" %% s"pekko-$name" % version
    Seq(
      pekkoModule("slf4j"),
      pekkoModule("actor"),
      pekkoModule("stream"),
      pekkoModule("persistence"),
      pekkoModule("persistence-typed"),
      pekkoModule("persistence-query"),
      pekkoModule("projection-core", pekkoProjectionSnapshotVersion),
      pekkoModule("projection-eventsourced", pekkoProjectionSnapshotVersion),
      pekkoModule("cluster-sharding-typed"),
      pekkoModule("cluster"),
      pekkoModule("coordination"),
      pekkoModule("cluster-tools"),
      pekkoModule("stream-testkit") % Test,
      pekkoModule("testkit") % Test,
      pekkoModule("actor-testkit-typed") % Test,
      pekkoModule("persistence-testkit") % Test,
      pekkoModule("projection-testkit",pekkoProjectionSnapshotVersion) % Test,
      "io.spray"                    %% "spray-json"                             % "1.3.6",
      "com.typesafe.scala-logging"  %% "scala-logging"                          % "3.9.6"
    )
  }

  val log = Seq(
      "ch.qos.logback" % "logback-classic" % "1.6.3",
      "net.logstash.logback" % "logstash-logback-encoder" % "8.1"
    )


  val  test = log ++ Seq(
      "org.scalatest"       %% "scalatest"                   % scalaTestVersion % Test
    )


  val pekkoHttpDeps = {
    def pekkoHttpModule(name: String, version: String = pekkoHttpVersion) =
      "org.apache.pekko" %% s"pekko-$name" % version

    baseDeps ++ Seq(
      pekkoHttpModule("http"),
      pekkoHttpModule("http-spray-json"),
      pekkoHttpModule("http-testkit") % Test
    ) ++ test
  }

  val testDeps = {
    baseDeps ++ Seq(
      "org.scalatest"          %% "scalatest"                 % scalaTestVersion,
      "org.apache.pekko"      %% "pekko-testkit"              % pekkoVersion,
      "org.apache.pekko"      %% "pekko-actor-testkit-typed"  % pekkoVersion,
      "org.apache.pekko"      %% "pekko-persistence-testkit"  % pekkoVersion,
      "org.apache.pekko"      %% "pekko-http-testkit"         % pekkoHttpVersion
    ) ++ test
  }

  /**
   * Databases for the offset store tests: H2 in-process for JDBC, Postgres (R2DBC) and Cassandra in containers.
   * H2 and the Postgres driver follow the case-engine versions.
   */
  val testcontainersVersion = "2.0.5"
  val offsetStoreTestDeps = Seq(
    "org.testcontainers" % "testcontainers-postgresql" % testcontainersVersion % Test,
    "org.testcontainers" % "testcontainers-cassandra"  % testcontainersVersion % Test,
    "org.postgresql"     % "postgresql"                % "42.7.13"             % Test,
    "com.h2database"     % "h2"                        % "2.5.250"             % Test
  )

  val persistenceLevelDBDeps = {
    baseDeps ++ Seq(
      "org.iq80.leveldb"            % "leveldb"        % "0.12",
      "org.fusesource.leveldbjni"   % "leveldbjni-all" % "1.8"
    )
  }

  val persistanceLmdbDBDeps = {
    baseDeps ++ Seq(
      "org.lmdbjava"                % "lmdbjava"        % "0.9.1"
    )
  }

  val persistenceInMemoryDeps = {
    baseDeps ++ Seq(
      "io.github.alstanchev" %% "pekko-persistence-inmemory" % "1.3.0" excludeAll ExclusionRule(organization = "org.apache.pekko")
    )
  }

  val persistenceCassandraDeps = {
    baseDeps ++ Seq(
      "org.apache.pekko" %% "pekko-persistence-cassandra" % "1.1.0"
    )
  }

  val persistenceJdbcDeps = {
    baseDeps ++ Seq(
      "org.apache.pekko"   %% "pekko-persistence-r2dbc" % "1.0.0",
      "org.apache.pekko"   %% "pekko-persistence-jdbc"  % "1.3.0",
      "com.typesafe.slick" %% "slick"                   % slickVersion,
    )
  }

  /**
   * Slick follows PVP, so sbt reads 3.5 -> 3.6 as potentially binary incompatible and fails the build.
   * pekko-persistence-jdbc is still built against Slick 3.5.x, but Slick 3.6.x is a drop-in for the API it uses.
   */
  val schemes: Seq[ModuleID] = Seq(
    "com.typesafe.slick" %% "slick"          % VersionScheme.Always,
    "com.typesafe.slick" %% "slick-hikaricp" % VersionScheme.Always
  )

  /**
   * Versions forced upon transitive dependencies that resolve to a version with known security issues.
   * Pekko and the Cassandra driver each bring in their own (older) Netty.
   */
  val overrides: Seq[ModuleID] = Seq(
    "buffer", "codec", "codec-dns", "codec-http", "codec-http2", "codec-socks", "common",
    "handler", "handler-proxy", "resolver", "resolver-dns", "resolver-dns-classes-macos",
    "resolver-dns-native-macos", "transport", "transport-classes-epoll", "transport-classes-kqueue",
    "transport-native-epoll", "transport-native-kqueue", "transport-native-unix-common"
  ).map(name => "io.netty" % s"netty-$name" % nettyVersion) ++ Seq(
    // Keep slick-hikaricp (via pekko-persistence-jdbc) on the same version as slick
    "com.typesafe.slick" %% "slick-hikaricp" % slickVersion
  )

}
