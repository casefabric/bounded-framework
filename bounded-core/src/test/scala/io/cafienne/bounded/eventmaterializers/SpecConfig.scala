/*
 * Copyright (C) 2016-2023 Batav B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.eventmaterializers

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
      |          default-dispatcher {
      |            executor = "fork-join-executor"
      |            fork-join-executor {
      |              parallelism-min = 8
      |              parallelism-factor = 2.0
      |              parallelism-max = 8
      |            }
      |          }
      |          serialize-creators = off
      |          serialize-messages = off
      |          serializers {
      |            //serializer = "io.cafienne.bounded.cargosample.persistence.CargoPersistersSerializer"
      |          }
      |          serialization-bindings {
      |            //"stamina.Persistable" = serializer
      |            // enable below to check if all events have been serialized without java.io.Serializable
      |            //"java.io.Serializable" = none
      |          }
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
      |    inmemory-journal {
      |      event-adapters {
      |        testTagging = "io.cafienne.bounded.eventmaterializers.TestTaggingEventAdapter"
      |      }
      |      event-adapter-bindings {
      |        "io.cafienne.bounded.aggregate.DomainEvent" = testTagging
      |      }
      |    }
      |    inmemory-read-journal {
      |      refresh-interval = "10ms"
      |      max-buffer-size = "1000"
      |    }
      |    bounded.eventmaterializers.publish = true
      |    bounded.eventmaterializers.offsetstore {
      |       type = "inmemory"
      |   }
    """.stripMargin
  )

}
