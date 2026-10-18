import sbt.Keys.localStaging

lazy val basicSettings = {
  val scala213 = "2.13.18"
  val supportedScalaVersions = List(scala213)

  Seq(
    organization := "io.cafienne.bounded",
    description := "Scala and Pekko based Domain Driven Design Framework",
    scalaVersion := scala213,
    //crossScalaVersions := supportedScalaVersions,
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
    publishMavenStyle := true,
    // Do not publish any test artifacts
    Test / publishArtifact := false,
    pomIncludeRepository := { _ => false },

    //When you add the file below to your .sbt folder, ask the repository manager for a set of credentials
    credentials += Credentials(Path.userHome / ".sbt" / "sonatype_central_credentials"),
    publishTo := {
      val centralSnapshots = "https://central.sonatype.com/repository/maven-snapshots/"
      if (isSnapshot.value) Some("central-snapshots" at centralSnapshots)
      else localStaging.value
    },

    /**
     * Version generation from the git tag (vX.Y.Z), depends on the [[GitInfoAnalyzer]]
     */
    git.useGitDescribe := true,
    git.gitTagToVersionNumber := { tag =>
      // This code sets the gitDescribedVersion from the current git tag.
      //  We do not actually return a version, but the version + the description.
      //  Next task is to pull them apart from each other again.
      //  This is done because somehow it is not possible to keep state inside the GitInfoAnalyzer object :(
      GitInfoAnalyzer.getVersionPlusDescription(tag, git.gitCurrentBranch.value, git.gitHeadCommit.value, git.gitUncommittedChanges.value)
    },
    // Now convert the generated gitDescribedVersion into a proper 'version'.
    // Unlike the case engine, 'description' is kept as is because it ends up in the published pom.
    version := GitInfoAnalyzer.version(version.value, git.gitDescribedVersion.value),
  )
}

lazy val boundedRoot = (project in file("."))
  .settings(basicSettings: _*)
  .settings(publishArtifact := false,
            publish / skip := true,
            crossScalaVersions := Nil)
  .enablePlugins(GitPlugin, GitVersioning, GitBranchPrompt, AutomateHeaderPlugin)
  .aggregate(boundedCore, boundedPekkoHttp, boundedTest)

val boundedCore = (project in file("bounded-core"))
  .enablePlugins(BuildInfoPlugin, GitPlugin, GitVersioning, GitBranchPrompt, AutomateHeaderPlugin)
  .settings(basicSettings: _*)
  .settings(
    name := "bounded-core",
    // Generate io.cafienne.bounded.BuildInfo with 'name', 'organization', 'version', 'description' and the git status
    buildInfoKeys := Seq[BuildInfoKey](
      name,
      organization,
      version,
      BuildInfoKey.action("buildDescription")(
        GitInfoAnalyzer.description(description.value, git.gitDescribedVersion.value)
      ),
      git.gitHeadCommit,
      git.gitCurrentBranch,
      git.gitUncommittedChanges
    ),
    buildInfoOptions += BuildInfoOption.BuildTime,
    buildInfoOptions += BuildInfoOption.ToMap,
    buildInfoPackage := "io.cafienne.bounded",
    buildInfoObject := "BuildInfo",
    libraryDependencies ++= Dependencies.baseDeps ++ Dependencies.persistenceLevelDBDeps ++ Dependencies.persistenceInMemoryDeps ++ Dependencies.persistenceCassandraDeps ++ Dependencies.persistenceJdbcDeps ++ Dependencies.testDeps,
    // lmdbjava (LmdbOffsetStore) needs these opens on Java 17+, so the tests run in a forked JVM that has them
    Test / fork := true,
    Test / javaOptions ++= Seq(
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
    ),
  )

val boundedPekkoHttp = (project in file("bounded-pekko-http"))
  .dependsOn(boundedCore)
  .enablePlugins(GitPlugin, GitVersioning, GitBranchPrompt, AutomateHeaderPlugin)
  .settings(basicSettings: _*)
  .settings(
    name := "bounded-pekko-http",
    libraryDependencies ++= Dependencies.pekkoHttpDeps)

val boundedTest = (project in file("bounded-test"))
  .dependsOn(boundedCore)
  .enablePlugins(GitPlugin, GitVersioning, GitBranchPrompt, AutomateHeaderPlugin)
  .settings(basicSettings: _*)
  .settings(
    name := "bounded-test",
    libraryDependencies ++= Dependencies.testDeps)

