
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.6.2")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.4.4")
//addSbtPlugin("org.scala-sbt" % "sbt-maven-resolver" % "0.1.1")
addSbtPlugin("com.github.sbt" % "sbt-pgp" % "2.3.2")
addSbtPlugin("com.github.sbt" % "sbt-header" % "5.11.0")
addSbtPlugin("com.timushev.sbt" % "sbt-updates" % "0.7.1")
// sbt-updates is often also installed as a global plugin (~/.sbt/1.0/plugins); don't fail on an older global version
libraryDependencySchemes += "com.timushev.sbt" % "sbt-updates" % VersionScheme.Always
addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.13.2")
addSbtPlugin("com.github.sbt" % "sbt-git" % "2.2.0")
