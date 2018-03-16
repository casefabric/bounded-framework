// Copyright (C) 2018 the original author or authors.
// See the LICENSE file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package io.cafienne.bounded.cargosample

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
  val testConfigDVriendInMem = ConfigFactory.parseString(
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
      |            serializer = "io.cafienne.bounded.cargosample.persistence.CargoPersistersSerializer"
      |          }
      |          serialization-bindings {
      |            "stamina.Persistable" = serializer
      |            // enable below to check if all events have been serialized without java.io.Serializable
      |            "java.io.Serializable" = none
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
      |        cargoTagging = "io.cafienne.bounded.cargosample.persistence.CargoTaggingEventAdapter"
      |      }
      |      event-adapter-bindings {
      |        "io.cafienne.bounded.cargosample.domain.CargoDomainProtocol$CargoDomainEvent" = cargoTagging
      |      }
      |    }
      |    inmemory-read-journal {
      |      refresh-interval = "10ms"
      |      max-buffer-size = "1000"
      |    }
    """.stripMargin
  )

  /*
   * This configuration works for Aggregate Root Testing. (see above for explanation)
   */
  val testConfigAkkaInMem = ConfigFactory.parseString(
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
      |            serializer = "io.cafienne.bounded.cargosample.persistence.CargoPersistersSerializer"
      |          }
      |          serialization-bindings {
      |            "stamina.Persistable" = serializer
      |            // enable below to check if all events have been serialized without java.io.Serializable
      |            "java.io.Serializable" = none
      |          }
      |        }
      |      persistence {
      |       publish-confirmations = on
      |       publish-plugin-commands = on
      |       journal {
      |          plugin = "akka.persistence.journal.inmem"
      |       }
      |      }
      |      test {
      |        single-expect-default = 10s
      |        timefactor = 1
      |      }
      |    }
    """.stripMargin
  )

}
