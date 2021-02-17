/*
 * Copyright (C) 2016-2021 Cafienne B.V. <https://www.cafienne.io/bounded>
 */

package io.cafienne.bounded.aggregate

import java.time.OffsetDateTime
import akka.actor.testkit.typed.scaladsl.ScalaTestWithActorTestKit
import org.scalatest.flatspec.AsyncFlatSpecLike
import scala.concurrent.Future

class TypedClusteredSpec extends ScalaTestWithActorTestKit(s"""
    //akka.actor.provider = "cluster"
    //akka.cluster.seed-nodes = ["akka://TypedClusteredSpec@127.0.0.1:2552"]
    akka.persistence.publish-plugin-commands = on
    akka.persistence.journal.plugin = "akka.persistence.journal.inmem"
    akka.persistence.journal.inmem.test-serialization = on
    # snapshot store plugin is NOT defined, things should still work
    akka.persistence.snapshot-store.local.dir = "target/snapshots-${classOf[TypedClusteredSpec].getName}/"
    """) with AsyncFlatSpecLike {
  //NOTE: Be aware that the config of the TestKit contains the name of the Cluster based on the name of this Spec (TypedClusteredSpec).
  //      When you copy this for your own use, change the name of the cluser.seed-nodes.

  import TypedSimpleAggregate._

  implicit val commandValidator = new ValidateableCommand[SimpleAggregateCommand] {
    override def validate(cmd: SimpleAggregateCommand): Future[SimpleAggregateCommand] =
      Future.successful(cmd)
  }

  implicit val ec = system.executionContext

  behavior of "Typed Cluster"

  val commandMetaData = AggregateCommandMetaData(OffsetDateTime.now(), None)

  val creator = new SimpleAggregateManager()
//  val sharding = ClusterSharding(system)test
//
//  sharding.init(
//    Entity(creator.entityTypeKey)(createBehavior = entityContext => creator.behavior(entityContext.entityId))
//  )

  "ShardedCluster" should "Create a basic non-clustered aggregate and send command" in {
    val commandMetaData = AggregateCommandMetaData(OffsetDateTime.now(), None)

    val aggregateId = "test0"
    val creator     = new SimpleAggregateManager()
    val actorRef    = spawn(creator.behavior(aggregateId), "testactor")
    val probe       = testKit.createTestProbe[Response]()

    actorRef.tell(Create(aggregateId, commandMetaData, probe.ref))
    val answer = probe.expectMessage(OK)
    assert(answer.equals(OK))
  }

//  it should "send a command via sharding and wait for a Typed reply" in {
//
//    val aggregateId = "test1"
//
//    val probe     = testKit.createTestProbe[Response]()
//    val entityRef = sharding.entityRefFor(creator.entityTypeKey, aggregateId)
//    val answer    = entityRef.?(ref => Create(aggregateId, commandMetaData, ref))
//
//    answer.map { result =>
//      assert(result.isInstanceOf[OK.type])
//    }
//  }

}
