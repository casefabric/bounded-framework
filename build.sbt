
lazy val basicSettings = {
  val currentScalaVersion = "2.12.3"
  val scala211Version     = "2.11.11"

  Seq(
    organization := "io.cafienne.bounded",
    description := "Scala and Akka based Domain Driven Design Framework",
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
    ),
    scalastyleConfig := baseDirectory.value / "project/scalastyle-config.xml"
  )
}

lazy val moduleSettings = basicSettings ++ Seq(
  homepage := Some(url("https://github.com/cafienne/bounded-framework")),
  scmInfo := Some(ScmInfo(url("https://github.com/cafienne/bounded-framework"), "git@github.com:cafienne/bounded-framework.git")),
  developers := List(Developer("olger",
    "Olger Warnier",
    "olger.warnier@spronq.com",
    url("https://github.com/olger"))),
  licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0")),

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

lazy val boundedRoot = (project in file("."))
  .settings(basicSettings: _*)
  //.settings(releaseSettings)
  .aggregate(boundedCore, boundedTest)

val boundedCore = (project in file("bounded-core"))
  .enablePlugins(ReleasePlugin)
  .settings(moduleSettings: _*)
  .settings(libraryDependencies ++= Dependencies.baseDeps)

val boundedTest = (project in file("bounded-test"))
  .dependsOn(boundedCore)
  .enablePlugins(ReleasePlugin)
  .settings(moduleSettings: _*)
  .settings(libraryDependencies ++= Dependencies.baseDeps)
