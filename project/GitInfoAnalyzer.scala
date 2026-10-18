object GitInfoAnalyzer {
  private val SPLITTER: String = "---"

  case class Version(baseVersion: Seq[Int], isSnapshot: Boolean) {
    val major: Int = baseVersion.head
    val minor: Int = baseVersion(1)
    val patch: Int = baseVersion(2)

    override val toString: String = {
      if (isSnapshot) {
        s"$major.$minor.${patch + 1}-SNAPSHOT"
      } else {
        s"$major.$minor.$patch"
      }
    }
  }

  def version(sbtVersion: String, gitDescribedVersion: Option[String]): String = {
    if (gitDescribedVersion.isEmpty) {
      println("Missing gitDescribedVersion; return 0.0.0")
      sbtVersion
    } else {
      val versionAndDescriptionSplitted = gitDescribedVersion.get.split(SPLITTER)
      if (versionAndDescriptionSplitted.length < 2) {
        // That's weird.
        println(s"Git described version '${gitDescribedVersion.get}' does not contain the expected SPLITTER $SPLITTER, returning default version")
        sbtVersion
      } else {
        versionAndDescriptionSplitted(0)
      }
    }
  }

  /**
    * Return BuildInfoKey with the description
    * @return
    */
  def description(sbtDescription: String, gitDescribedVersion: Option[String]): String = {
    println("SBT Description: " + sbtDescription)
    if (gitDescribedVersion.isEmpty) {
      println("Missing gitDescribedVersion; return 0.0.0")
      sbtDescription
    } else {
      val versionAndDescriptionSplitted = gitDescribedVersion.get.split(SPLITTER)
      if (versionAndDescriptionSplitted.length < 2) {
        // That's weird.
        println(s"Git described version '${gitDescribedVersion.get}' does not contain the expected SPLITTER $SPLITTER, returning default version")
        sbtDescription
      } else {
        versionAndDescriptionSplitted(1)
      }
    }
  }

  /**
    * Analyze the given information on git source code status
    */
  def getVersionPlusDescription(currentGitTag: String, gitBranch: String, gitHeadCommit: Option[String], localChanges: Boolean): Option[String] = {
    // Release tags of bounded are prefixed with a 'v' (e.g. v0.3.7)
    val dashedVersions = currentGitTag.stripPrefix("v").split("-")

    val baseline = dashedVersions(0)
    val gitCommit = gitHeadCommit.getOrElse("")
    val additionalCommits = {
      if (dashedVersions.length > 1) dashedVersions(1).toInt
      else 0
    }
    val isSnapshot = additionalCommits > 0 || localChanges;

    println(s"--- Analyzing Git Information")
    println(s" - git tag given:         $currentGitTag")
    println(s" - base version:          $baseline")
    println(s" - branch:                $gitBranch")
    println(s" - commits in branch:     $additionalCommits")
    println(s" - current commit:        ${gitHeadCommit.getOrElse("no commit has been done")}")
    println(s" - found local changes:   $localChanges")
    println(s" - is snapshot:           $isSnapshot\n")

    val version = Version(dashedVersions(0).split("[.]").map(_.toInt), isSnapshot)

    val description = {
      if (additionalCommits > 0) {
        if (localChanges) {
          s"This library is built on commit [$gitCommit] in branch [$gitBranch] having $additionalCommits additional commits on top of version [$baseline]. The build also contains some local changes that have not been committed."
        } else {
          s"This library is built on commit [$gitCommit] in branch [$gitBranch] having $additionalCommits additional commits on top of version [$baseline]."
        }
      } else {
        if (localChanges) {
          s"This library is built on branch [$gitBranch] with tag [$baseline] and some local changes that have not been committed."
        } else {
          s"This library is built on branch [$gitBranch] with tag [$baseline]."
        }
      }
    }

    println("--- Resulting Description and Version")
    println(" -  description: " + description)
    println(" -  version:     " + version)
    println("")

    Some(version + SPLITTER + description)
  }
}
