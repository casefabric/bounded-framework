/*
 * Copyright (C) 2016-2026 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.test

import io.cafienne.bounded.serialization.VersionedJsonSerializer
import org.scalatest.Assertions
import org.scalatest.Assertion
import spray.json._

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

/**
  * Checks that a `VersionedJsonSerializer` still reads every event it ever stored. Keep one stored sample per event
  * version in a folder, named by its manifest: `<folder>/<EventName>.v<n>.json`, holding exactly the bytes the
  * serializer wrote (with `{"meta": …, "data": …}` for an event that carries metadata). Never change a fixture of an
  * old version; add one for every new version (`write` creates them from sample events).
  *
  * {{{
  * class OrderEventsSpec extends AnyWordSpec with EventFixtureChecks {
  *   "the order events" should {
  *     "read every stored version" in assertEventFixtures(new OrderSerializer, Paths.get("src/test/resources/events"))
  *   }
  * }
  * }}}
  */
object EventFixtures {

  /** A fixture that failed: the serializer could not read it, or wrote the current version differently. */
  final case class FixtureFailure(file: Path, manifest: String, reason: String)

  /**
    * @param checked  the manifests of the fixtures that were checked
    * @param failures the fixtures that failed
    * @param missing  the event versions of the serializer without a fixture
    * @param unknown  fixtures whose manifest the serializer does not know
    */
  final case class FixtureReport(
    checked: Seq[String],
    failures: Seq[FixtureFailure],
    missing: Seq[String],
    unknown: Seq[Path]
  ) {
    def isOk: Boolean = failures.isEmpty && missing.isEmpty && unknown.isEmpty

    override def toString: String =
      (failures.map(f => s"${f.file.getFileName}: ${f.reason}") ++
        missing.map(m => s"no fixture for $m") ++
        unknown.map(p => s"${p.getFileName}: the serializer has no event version ${manifestOf(p)}"))
        .mkString("\n")
  }

  /**
    * Reads every fixture with the serializer. A fixture of the current version must also be written back as the
    * same JSON (compared as JSON, so formatting does not matter); an older version must be readable.
    */
  def verify(serializer: VersionedJsonSerializer, folder: Path): FixtureReport = {
    val files                 = fixtureFiles(folder)
    val known                 = serializer.manifests.toSet
    val (recognized, unknown) = files.partition(file => known.contains(manifestOf(file)))
    val failures              = recognized.flatMap(file => check(serializer, file))
    FixtureReport(
      recognized.map(manifestOf),
      failures,
      serializer.manifests.filterNot(files.map(manifestOf).toSet),
      unknown
    )
  }

  /**
    * Writes a fixture of the current version for every sample event that has none yet, and returns the files it
    * wrote. Existing fixtures are never overwritten.
    */
  def write(serializer: VersionedJsonSerializer, folder: Path, samples: Seq[AnyRef]): Seq[Path] = {
    Files.createDirectories(folder)
    samples.flatMap { sample =>
      val file = folder.resolve(s"${serializer.manifest(sample)}.json")
      if (Files.exists(file)) None
      else {
        Files.write(file, serializer.toBinary(sample)): Unit
        Some(file)
      }
    }
  }

  private def check(serializer: VersionedJsonSerializer, file: Path): Option[FixtureFailure] = {
    val manifest = manifestOf(file)
    try {
      val bytes = Files.readAllBytes(file)
      val event = serializer.fromBinary(bytes, manifest)
      if (serializer.manifest(event) != manifest) None // an older version: reading it is enough
      else {
        val written = parse(serializer.toBinary(event))
        val stored  = parse(bytes)
        if (written == stored) None
        else
          Some(
            FixtureFailure(file, manifest, s"written back as ${written.compactPrint}, stored ${stored.compactPrint}")
          )
      }
    } catch {
      case NonFatal(e) => Some(FixtureFailure(file, manifest, s"${e.getClass.getSimpleName}: ${e.getMessage}"))
    }
  }

  private def parse(bytes: Array[Byte]): JsValue = new String(bytes, StandardCharsets.UTF_8).parseJson

  private def fixtureFiles(folder: Path): Seq[Path] =
    if (!Files.isDirectory(folder)) Seq.empty
    else {
      val stream = Files.list(folder)
      try stream.iterator().asScala.filter(_.getFileName.toString.endsWith(".json")).toList.sortBy(_.toString)
      finally stream.close()
    }

  private def manifestOf(file: Path): String = file.getFileName.toString.stripSuffix(".json")
}

/** `EventFixtures.verify` as a ScalaTest assertion. */
trait EventFixtureChecks { this: Assertions =>

  def assertEventFixtures(serializer: VersionedJsonSerializer, folder: Path): Assertion = {
    val report = EventFixtures.verify(serializer, folder)
    if (report.isOk) succeed
    else fail(s"Event fixtures in $folder do not match ${serializer.getClass.getName}:\n$report")
  }
}
