import xerial.sbt.Sonatype.sonatypeCentralHost

lazy val basicSettings = {
  val scala213 = "2.13.18"
  val supportedScalaVersions = List(scala213)

  Seq(
    organization := "io.cafienne.bounded",
    description := "Scala and Pekko based Domain Driven Design Framework",
    scalaVersion := scala213,
    //crossScalaVersions := supportedScalaVersions,
    //releaseCrossBuild := false,
    scalacOptions := Seq(
      "-encoding", "UTF-8",
      //"-target:jvm-1.8",
      "-deprecation", // warning and location for usages of deprecated APIs
      "-feature", // warning and location for usages of features that should be imported explicitly
      "-unchecked", // additional warnings where generated code depends on assumptions
      "-Xlint", // recommended additional warnings
      "-Ywarn-value-discard", // Warn when non-Unit expression results are unused
      "-Ywarn-dead-code",
      "-Ywarn-unused" //,
      //"-Xsource:3" //,
      //"-Ywarn-unused-import"
    ),
    //scalastyleConfig := baseDirectory.value / "project/scalastyle-config.xml",
    scalafmtConfig := (ThisBuild / baseDirectory).value / "project/.scalafmt.conf",
    scalafmtOnCompile := true,
    // Force known-good versions on transitive dependencies, see [[Dependencies.overrides]]
    dependencyOverrides ++= Dependencies.overrides,
    // See [[Dependencies.schemes]]
    libraryDependencySchemes ++= Dependencies.schemes,

    startYear := Some(2018),
    licenses := Seq("Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0")),
    headerLicense := {
      val toYear = java.time.Year.now
      Some(HeaderLicense.Custom(
        s"Copyright (C) 2016-$toYear Batav B.V. <https://www.cafienne.io/bounded>"
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
    versionScheme := Some("early-semver"),
    // Add sonatype repository settings
    publishTo := {
      val centralSnapshots = "https://central.sonatype.com/repository/maven-snapshots/"
        if (isSnapshot.value) Some("central-snapshots" at centralSnapshots)
        else localStaging.value
    },
    credentials += Credentials(Path.userHome / ".sbt" / "sonatype_central_credentials"),
    sonatypeCredentialHost := sonatypeCentralHost,
    publishMavenStyle := true,
    Test / publishArtifact := false,
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
  .aggregate(boundedCore, boundedPekkoHttp, boundedTest)

val boundedCore = (project in file("bounded-core"))
  .enablePlugins(ReleasePlugin, AutomateHeaderPlugin)
  .settings(basicSettings: _*)
  .settings(
    name := "bounded-core",
    libraryDependencies ++= Dependencies.baseDeps ++ Dependencies.persistenceLevelDBDeps ++ Dependencies.persistenceInMemoryDeps ++ Dependencies.persistenceCassandraDeps ++ Dependencies.persistenceJdbcDeps ++ Dependencies.testDeps,
  )

val boundedPekkoHttp = (project in file("bounded-pekko-http"))
  .dependsOn(boundedCore)
  .enablePlugins(ReleasePlugin, AutomateHeaderPlugin)
  .settings(basicSettings: _*)
  .settings(
    name := "bounded-pekko-http",
    libraryDependencies ++= Dependencies.pekkoHttpDeps)

val boundedTest = (project in file("bounded-test"))
  .dependsOn(boundedCore)
  .enablePlugins(ReleasePlugin, AutomateHeaderPlugin)
  .settings(basicSettings: _*)
  .settings(
    name := "bounded-test",
    libraryDependencies ++= Dependencies.testDeps)

