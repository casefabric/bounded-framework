/*
 * Copyright (C) 2016-2023 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.test

import com.typesafe.config.ConfigFactory

object SpecConfig {

  /*
  PLEASE NOTE:
  Currently the https://github.com/dnvriend/akka-persistence-inmemory is NOT working for Aggregate Root tests
  because it is not possible to use a separate instance writing the events that should be in the event store
  before you actually create the aggregate root (should replay those stored events) to check execution of a new
  command.
  A new configuration that uses the akka bundled inmem storage is added to create a working situation.
   */
  val testConfig = ConfigFactory.parseString(
    """
      |      akka {
      |        loglevel = "DEBUG"
      |        stdout-loglevel = "DEBUG"
      |        loggers = ["akka.testkit.TestEventListener"]
      |        actor {
      |          serialize-messages = off
      |          serialize-creators = off
      |          allow-java-serialization = on
      |          default-dispatcher {
      |            executor = "fork-join-executor"
      |            fork-join-executor {
      |              parallelism-min = 8
      |              parallelism-factor = 2.0
      |              parallelism-max = 8
      |            }
      |          }
      |          //serialize-creators = off
      |          //serialize-messages = off
      |        }
      |      persistence {
      |       publish-confirmations = on
      |       publish-plugin-commands = on
      |       journal {
      |          plugin = "inmemory-journal"
      |       }
      |       snapshot-store.plugin = "inmemory-snapshot-store"
      |      }
      |      test {
      |        single-expect-default = 10s
      |        timefactor = 1
      |      }
      |    }
      |    inmemory-read-journal {
      |      refresh-interval = "10ms"
      |      max-buffer-size = "1000"
      |    }
      |
      |    bounded.eventmaterializers.publish = true
      |
      |    bounded.eventmaterializers.offsetstore {
      |       type = "inmemory"
      |   }
    """.stripMargin
  )

}
