import sbt._

object Dependencies {

  val baseDeps = {
    def akkaModule(name: String, version: String = "2.5.9") =
      "com.typesafe.akka" %% s"akka-$name" % version
    Seq(
      akkaModule("slf4j"),
      akkaModule("actor"),
      akkaModule("stream"),
      akkaModule("persistence"),
      akkaModule("persistence-query"),
      akkaModule("stream-testkit") % Test,
      akkaModule("testkit") % Test,
      "com.scalapenos"                  %% "stamina-json"                           % "0.1.3",
      "io.spray"                        %% "spray-json"                             % "1.3.3",
//      "org.iq80.leveldb"                %  "leveldb"                                % "0.8",
//      "org.fusesource.leveldbjni"       %  "leveldbjni-all"                         % "1.8",
      "com.typesafe.akka"               %% "akka-persistence-cassandra"             % "0.54",
      "com.typesafe.scala-logging"      %% "scala-logging"                          % "3.5.0"
    )
  }

  val akkaHttpDeps = {
    def akkaHttpModule(name: String, version: String = "10.0.11") =
      "com.typesafe.akka" %% s"akka-$name" % version

    baseDeps ++ Seq(
      akkaHttpModule("http"),
      akkaHttpModule("http-spray-json"),
      akkaHttpModule("http-testkit") % Test
    )
  }

  val testDeps = {
    baseDeps ++ Seq(
      "org.scalatest"          %% "scalatest"                   % "3.0.1",
      "com.github.dnvriend"    %% "akka-persistence-inmemory"   % "2.5.0.0",
      "com.typesafe.akka"      %% "akka-testkit"                % "2.5.9"
    )
  }

  val log = {
    Seq(
      "ch.qos.logback" % "logback-classic" % "1.2.3",
      "net.logstash.logback" % "logstash-logback-encoder" % "4.10"
    )
  }

  val  test = {
    log ++ Seq(
      "org.scalatest"       %% "scalatest"                   % "3.0.1" % Test,
      "com.github.dnvriend" %% "akka-persistence-inmemory"   % "2.5.0.0" % Test,
      "com.scalapenos"      %% "stamina-testkit"             % "0.1.3" % Test,
      "org.scalamock"       %% "scalamock-scalatest-support" % "3.6.0" % Test,
      "com.danielasfregola" %% "random-data-generator"       % "2.3" % Test
    )
  }

}
