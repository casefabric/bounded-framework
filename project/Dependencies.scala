import sbt._

object Dependencies {

  val scalaV = "2.11.8"

  val baseDeps = {
    def akkaModule(name: String, version: String = "2.5.9") =
      "com.typesafe.akka" %% s"akka-$name" % version
    def akkaHttpModule(name: String, version: String = "10.0.11") =
      "com.typesafe.akka" %% s"akka-$name" % version
    Seq(
      akkaModule("slf4j"),
      akkaModule("actor"),
      akkaModule("stream"),
      akkaModule("persistence"),
      akkaModule("persistence-query"),
      akkaModule("stream-testkit"), //% "bounded.test",
      akkaModule("testkit"), // % "bounded.test",
      akkaHttpModule("http"),
      akkaHttpModule("http-spray-json"),
      akkaHttpModule("http-testkit"), //% "bounded.test",
      "com.github.dnvriend"             %% "akka-persistence-inmemory"              % "2.5.0.0", // % "bounded.test",
      "com.scalapenos"                  %% "stamina-testkit"                        % "0.1.3", // % "bounded.test",
      "com.scalapenos"                  %% "stamina-json"                           % "0.1.3",
      "io.spray"                        %% "spray-json"                             % "1.3.3",
      "org.iq80.leveldb"                %  "leveldb"                                % "0.8",
      "org.fusesource.leveldbjni"       %  "leveldbjni-all"                         % "1.8",
      "com.typesafe.akka"               %% "akka-persistence-cassandra"             % "0.54",
      "com.github.swagger-akka-http"    %% "swagger-akka-http"                      % "0.10.0",
      "org.scalatest"                   %% "scalatest"                              % "3.0.1", //    % "bounded.test"
      "com.github.j5ik2o"               %  "sw4jj_2.11"                             % "1.0.2",
      "com.typesafe.scala-logging"      %% "scala-logging"                          % "3.5.0"
    )
  }

  object log {
    val logback = "ch.qos.logback" % "logback-classic" % "1.2.3"
    val logstash = "net.logstash.logback" % "logstash-logback-encoder" % "4.10"
    val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0"
  }

  object test {
    val scalamock =  "org.scalamock" %% "scalamock-scalatest-support" % "3.6.0" % Test
    val randomDataGenerator = "com.danielasfregola" %% "random-data-generator" % "2.3" % Test
  }

}
