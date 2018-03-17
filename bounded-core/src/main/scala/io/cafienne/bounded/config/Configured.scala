/*
 * Copyright (C) 2016-2018 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.config

import com.typesafe.config.{ConfigFactory, Config}

trait Configured {
  def config: Config = defaultConfig

  private lazy val defaultConfig = {
    val fallback = ConfigFactory.defaultReference()
    ConfigFactory.load().withFallback(fallback)
  }

  /**
    * Check if a feature is enabled or not. Use this to en/disable features for production, e.g. a feature that is implemented in the backend, but the frontend needs to be adjusted.
    *
    * @param featurePath configuration property path: pp.features.XXX = <Boolean>
    * @param defaultState default state of feature when not configured. Defaults to true
    * @return boolean indicating whether the feauture is enabled.
    */
  def isFeatureEnabled(featurePath: String,
                       defaultState: Boolean = true): Boolean =
    (config.hasPath(featurePath) && config.getBoolean(featurePath)) || (!config
      .hasPath(featurePath) && defaultState)
}
