
lazy val basicSettings = {
  val currentScalaVersion = "2.12.3"
  val scala211Version     = "2.11.11"

  Seq(
    organization := "org.boundedframework",
    description := "Scala and Akka based Domain Driven Design Framework",
    licenses +=("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0")),
    scalaVersion := currentScalaVersion,
    crossScalaVersions := Seq(currentScalaVersion, scala211Version),
    scalacOptions := Seq(
      "-encoding", "UTF-8",
      "-target:jvm-1.8",
      "-deprecation", // warning and location for usages of deprecated APIs
      "-feature", // warning and location for usages of features that should be imported explicitly
      "-unchecked", // additional warnings where generated code depends on assumptions
      "-Xlint", // recommended additional warnings
      "-Xcheckinit", // runtime error when a val is not initialized due to trait hierarchies (instead of NPE somewhere else)
      "-Ywarn-adapted-args", // Warn if an argument list is modified to match the receiver
      "-Ywarn-value-discard", // Warn when non-Unit expression results are unused
      "-Ywarn-inaccessible",
      "-Ywarn-dead-code"
    )
  )
}

lazy val moduleSettings = basicSettings ++ Seq(
    publishMavenStyle := true,
    publishTo := {
        val nexus = "https://oss.sonatype.org/"
        if (isSnapshot.value)
            Some("snapshots" at nexus + "content/repositories/snapshots")
        else
            Some("releases"  at nexus + "service/local/staging/deploy/maven2")
    },
    publishArtifact in Test := false,
    pomIncludeRepository := { _ => false },
    pomExtra :=
      <url>https://github.com/bounded-ddd/bounded-framework</url>
      <issueManagement>
          <system>Github</system>
          <url>https://github.com/bounded-ddd/bounded-framework/issues</url>
      </issueManagement>
      <scm>
          <url>git@github.com:bounded-ddd/bounded-framework.git</url>
          <connection>scm:git:git@github.com:bounded-ddd/bounded-framework.git</connection>
      </scm>
      <developers>
        <developer>
          <id>olger</id>
          <name>Olger Warnier</name>
            <email>olger@spectare.nl</email>
        </developer>
      </developers>
) 

lazy val boundedRoot = (project in file("."))
  .settings(basicSettings: _*)
  .enablePlugins(ReleasePlugin)
  //.settings(releaseSettings)
  .aggregate(boundedCore, boundedTest)

val boundedCore = (project in file("bounded-core"))
  .settings(moduleSettings: _*)
  .settings(libraryDependencies ++= Dependencies.baseDeps)

val boundedTest = (project in file("bounded-test"))
  .dependsOn(boundedCore)
  .settings(moduleSettings: _*)
  .settings(libraryDependencies ++= Dependencies.baseDeps)
