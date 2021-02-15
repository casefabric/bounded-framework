/*
 * Copyright (C) 2016-2021 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.runtime

import java.io.{File, FileWriter}
import java.util.UUID

import io.cafienne.bounded.RuntimeInfo

import scala.io.Source

/**
  * <p>A PerLocalInstanceRuntimeInfoLoader ensures that for a single deployment (instance) the same runtime id is used even after a restart.
  * This allows event materializers to use a {@link io.cafienne.bounded.eventmaterializers.MaterializerEventFilter} to
  * filter on event that where created on a specific instance.</p>
  *
  * <p>Sample implementation:
  * <blockquote><pre>
  *   val filename             = system.settings.config.getString("application.runtimeinfo.path")
  *   implicit val runtimeInfo = PerLocalInstanceRuntimeInfoLoader(new File(filename))
  * </pre></blockquote></p>
  */
object PerLocalInstanceRuntimeInfoLoader {

  private def newId = UUID.randomUUID().toString.replaceAll("-", "")

  def apply(file: File): RuntimeInfo = {
    val id = if (file.exists()) {
      Source.fromFile(file).getLines().mkString
    } else {
      val runtimeId = newId
      val fw        = new FileWriter(file)
      fw.write(runtimeId)
      fw.close()
      runtimeId
    }
    RuntimeInfo(id)
  }

}
