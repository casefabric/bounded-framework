import sbt._

object Dependencies {

  val akkaVersion = "2.5.11"
  val staminaVersion = "0.1.4"
  val persistenceInMemVersion = "2.5.1.1"

  val baseDeps = {
    def akkaModule(name: String, version: String = akkaVersion) =
      "com.typesafe.akka" %% s"akka-$name" % version
    Seq(
      akkaModule("slf4j"),
      akkaModule("actor"),
      akkaModule("stream"),
      akkaModule("persistence"),
      akkaModule("persistence-query"),
      akkaModule("stream-testkit") % Test,
      akkaModule("testkit") % Test,
      "com.scalapenos"              %% "stamina-json"                           % staminaVersion,
      "io.spray"                    %% "spray-json"                             % "1.3.4",
      "com.typesafe.akka"           %% "akka-persistence-cassandra"             % "0.83",
      "com.github.dnvriend"         %% "akka-persistence-inmemory"              % persistenceInMemVersion,
      "com.typesafe.scala-logging"  %% "scala-logging"                          % "3.5.0"
    )
  }

  val log = Seq(
      "ch.qos.logback" % "logback-classic" % "1.2.3",
      "net.logstash.logback" % "logstash-logback-encoder" % "4.10"
    )


  val  test = log ++ Seq(
      "org.scalatest"       %% "scalatest"                   % "3.0.1" % Test,
      "com.scalapenos"      %% "stamina-testkit"             % "0.1.4" % Test,
      "org.scalamock"       %% "scalamock-scalatest-support" % "3.6.0" % Test
    )


  val akkaHttpDeps = {
    def akkaHttpModule(name: String, version: String = "10.1.0") =
      "com.typesafe.akka" %% s"akka-$name" % version

    baseDeps ++ Seq(
      akkaHttpModule("http"),
      akkaHttpModule("http-spray-json"),
      akkaHttpModule("http-testkit") % Test,
      "io.swagger" % "swagger-jaxrs" % "1.5.16",
      // As suggested in https://stackoverflow.com/questions/43574426/how-to-resolve-java-lang-noclassdeffounderror-javax-xml-bind-jaxbexception-in-j
      // to resolve blow-up due to swagger :  java.lang.NoClassDefFoundError: javax/xml/bind/annotation/XmlRootElement.
      "javax.xml.bind" % "jaxb-api" % "2.3.0",
      "com.github.swagger-akka-http" %% "swagger-akka-http" % "0.14.0"
    ) ++ test
  }

  val testDeps = {
    baseDeps ++ Seq(
      "org.scalatest"          %% "scalatest"                   % "3.0.1",
      "com.typesafe.akka"      %% "akka-testkit"                % akkaVersion
    ) ++ test
  }

  val persistenceLevelDBDeps = {
    baseDeps ++ Seq(
      "org.iq80.leveldb"            % "leveldb"        % "0.9",
      "org.fusesource.leveldbjni"   % "leveldbjni-all" % "1.8"
    )
  }

}
