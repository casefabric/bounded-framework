
lazy val basicSettings = {
  val scala213 = "2.13.0"
  val scala212 = "2.12.9"
  val supportedScalaVersions = List(scala213, scala212)

  Seq(
    organization := "io.cafienne.bounded",
    description := "Scala and Akka based Domain Driven Design Framework",
    scalaVersion := scala212,
    crossScalaVersions := supportedScalaVersions,
    releaseCrossBuild := true,
    scalacOptions := Seq(
      "-encoding", "UTF-8",
      "-target:jvm-1.8",
      "-deprecation", // warning and location for usages of deprecated APIs
      "-feature", // warning and location for usages of features that should be imported explicitly
      "-unchecked", // additional warnings where generated code depends on assumptions
      "-Xlint", // recommended additional warnings
      "-Ywarn-value-discard", // Warn when non-Unit expression results are unused
      "-Ywarn-dead-code",
      "-Ywarn-unused",
      "-Ywarn-unused-import"
    ),
    scalastyleConfig := baseDirectory.value / "project/scalastyle-config.xml",
    scalafmtConfig := Some((baseDirectory in ThisBuild).value / "project/.scalafmt.conf"),
    scalafmtOnCompile := true,

    startYear := Some(2018),
    licenses := Seq("Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0")),
    headerLicense := {
      val toYear = java.time.Year.now
      Some(HeaderLicense.Custom(
        s"Copyright (C) 2016-$toYear Cafienne B.V. <https://www.cafienne.io/bounded>"
      ))
    },
    homepage := Some(url("https://github.com/cafienne/bounded-framework")),
    scmInfo := Some(ScmInfo(
      url("https://github.com/cafienne/bounded-framework"),
      "git@github.com:cafienne/bounded-framework.git")
    ),
    developers := List(Developer(
      "olger",
      "Olger Warnier",
      "olger@spectare.nl",
      url("https://github.com/olger"))
    ),
    // Add sonatype repository settings
    publishTo := Some(
      if (isSnapshot.value)
        Opts.resolver.sonatypeSnapshots
      else
        Opts.resolver.sonatypeStaging
    ),
    publishMavenStyle := true,
    publishArtifact in Test := false,
    pomIncludeRepository := { _ => false },
  )
}

lazy val boundedRoot = (project in file("."))
  .settings(basicSettings: _*)
  .settings(publishArtifact := false,
            publish / skip := true,
            crossScalaVersions := Nil)
  .enablePlugins(AutomateHeaderPlugin)
  //.settings(releaseSettings)
  .aggregate(boundedCore, boundedAkkaHttp, boundedTest)

val boundedCore = (project in file("bounded-core"))
  .enablePlugins(ReleasePlugin, AutomateHeaderPlugin)
  .settings(basicSettings: _*)
  .settings(
    name := "bounded-core",
    libraryDependencies ++= Dependencies.baseDeps ++ Dependencies.persistanceLmdbDBDeps ++ Dependencies.persistenceCassandraDeps ++ Dependencies.testDeps)

val boundedAkkaHttp = (project in file("bounded-akka-http"))
  .dependsOn(boundedCore)
  .enablePlugins(ReleasePlugin, AutomateHeaderPlugin)
  .settings(basicSettings: _*)
  .settings(
    name := "bounded-akka-http",
    libraryDependencies ++= Dependencies.akkaHttpDeps)

val boundedTest = (project in file("bounded-test"))
  .dependsOn(boundedCore)
  .enablePlugins(ReleasePlugin, AutomateHeaderPlugin)
  .settings(basicSettings: _*)
  .settings(
    name := "bounded-test",
    libraryDependencies ++= Dependencies.testDeps)

