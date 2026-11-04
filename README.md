## Domain driven design framework for Scala and Pekko

Bounded is a framework that supports the Domain Driven Design approach. It enables you to leverage Scala and Apache Pekko technologies in a way that gives you a structured DDD based application, built on event sourcing and CQRS.
[![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=casefabric_bounded-framework&metric=reliability_rating)](https://sonarcloud.io/summary/new_code?id=casefabric_bounded-framework)

The flow through an application is:

```
command → command gateway → aggregate root (stores events) → event journal → event materializers → query stores
```

- An **aggregate root** handles the commands for one aggregate and stores the resulting events.
- A **command gateway** starts the aggregate root a command is for, validates the command and delivers it.
- **Event materializers** (projections) read the stored events of an aggregate type and build query stores from them; **combined logic** reacts to the events of several aggregate types.

```
                 ┌─────────┐   ┌──────────────────────┐
                 │         │   │      DOMAIN          │
┌───────┐        │ Command │   │        ┌───────┐     │
│       │        │ Gateway │   │        │       │     │
│       │────────▶         ├───┼────────▶  AR   │─────┼────────────────┐
│       │        │         │   │        │       │     │                │
│  API  │        │         │   │        └───────┘     │                ▼
│       │        └─────────┘   │                      │       ┌────────────────┐
│       │                      └──────────────────────┘       │     EVENT      │
│       │                                                     │     JOURNAL    │
│       │◀─────────┐           ┌──────────────────────┐       │                │
│       │          │           │   Materializers      │       └────────────────┘
└───────┘          │           │┌─────────┬─────────┐ │                │
                   │           ││ Query   │ Query   │ │                │
                   └───────────┤│ store   │ writer  │◀┼────────────────┘
                               │└─────────┴─────────┘ │
                               └──────────────────────┘
AR = Aggregate Root
```

Because every change is an event, the journal holds the full history of the application. A new query store can be built later by replaying the events, and an existing one can be rebuilt when its model changes.

## Getting started

Bounded is built for Scala 3 and published to Maven Central:

```scala
libraryDependencies ++= Seq(
  "io.cafienne.bounded" %% "bounded-core"       % boundedVersion,
  "io.cafienne.bounded" %% "bounded-test"       % boundedVersion % Test, // test helpers
  "io.cafienne.bounded" %% "bounded-pekko-http" % boundedVersion,        // optional: Pekko HTTP helpers
  "io.cafienne.bounded" %% "bounded-pekko-projection" % boundedVersion    // optional: run read models as Pekko Projections
)
```

All configuration uses Pekko's `pekko.` keys; `akka.` keys are silently ignored.

## Aggregate types

Every aggregate has an `AggregateType`. Define it once and use it on both the write side (the aggregate) and the read side (materializers and combined logic):

```scala
object Cargo {
  val Type: AggregateType = AggregateType("cargo")
}
```

The name is part of every stored persistence id (`cargo|<aggregate id>`), so it can never change without migrating the journal. It must be non-empty and must not contain `|`; the aggregate id may contain `|`. To get the aggregate id back from a persistence id, use `AggregateType.aggregateIdOf(persistenceId)`, not Pekko's `persistenceId.entityId`: that one reads `cargo|a|b` as a replication id.

## Commands, events and state

Commands carry the id of the aggregate root they are for, and events the id of the aggregate root that stored them:

```scala
final case class PlanCargo(aggregateRootId: String, trackingId: TrackingId, routeSpecification: RouteSpecification)
    extends DomainCommand
final case class SpecifyNewRoute(aggregateRootId: String, routeSpecification: RouteSpecification)
    extends DomainCommand

final case class CargoPlanned(id: String, trackingId: TrackingId, routeSpecification: RouteSpecification)
    extends DomainEvent
final case class NewRouteSpecified(id: String, routeSpecification: RouteSpecification) extends DomainEvent
```

## Writing an aggregate root

Bounded supports two styles of aggregate root. Both can be used in one application.

### Classic aggregate roots

A classic aggregate root extends `AggregateRootActor`, a Pekko classic `PersistentActor`. This is the Cargo example from Eric Evans' book:

```scala
class Cargo(cargoId: String) extends AggregateRootActor[CargoState] {

  override def aggregateId: String          = cargoId
  override def aggregateType: AggregateType = Cargo.Type

  override def handleCommand(command: DomainCommand, state: Option[CargoState]): Reply =
    command match {
      case cmd: PlanCargo if state.isEmpty =>
        Ok(Seq(CargoPlanned(cmd.aggregateRootId, cmd.trackingId, cmd.routeSpecification)))
      case cmd: SpecifyNewRoute if state.isDefined =>
        Ok(Seq(NewRouteSpecified(cmd.aggregateRootId, cmd.routeSpecification)))
      case other =>
        Ko(new UnexpectedCommand(other))
    }

  override def newState(evt: DomainEvent): Option[CargoState] =
    evt match {
      case CargoPlanned(_, trackingId, routeSpecification) => Some(CargoState(trackingId, routeSpecification))
      case _                                               => None
    }
}
```

`handleCommand` decides, and returns a `Reply`: `Ok(events)` or `Ko(failure)`. Bounded stores the events of an `Ok` before it sends the reply, and updates the state. `newState` creates the state from the first event; after that the state evolves through `AggregateState.update`:

```scala
final case class CargoState(trackingId: TrackingId, routeSpecification: RouteSpecification)
    extends AggregateState[CargoState] {

  override def update(evt: DomainEvent): Option[CargoState] =
    evt match {
      case NewRouteSpecified(_, routeSpecification) => Some(copy(routeSpecification = routeSpecification))
      case _                                        => Some(this)
    }
}
```

Optional settings:
- `snapShotInterval`: save a snapshot every n events (older snapshots are deleted).
- `tags(event)`: extra tags for an event. Only tag-based journals (see [Journals](#journals)) can query them.

A command gateway starts the aggregate root for a command when it is not running, validates the command, delivers it and stops aggregate roots that are idle:

```scala
val cargoCreator = new AggregateRootCreator {
  override def props(cargoId: String): Props = Props(new Cargo(cargoId))
}
val gateway = new RouterCommandGateway(system, idleTime = 10.minutes, cargoCreator)

implicit val planCargoValidator: ValidateableCommand[PlanCargo] = cmd => Future.successful(cmd)

gateway.sendAndAsk(PlanCargo(cargoId, trackingId, routeSpecification)) // Future of the Reply: Ok(events) or Ko(failure)
```

A validator that fails its `Future` rejects the command with a `CommandValidationException`; the command then never reaches the aggregate root.

### Typed aggregate roots

A typed aggregate root is a Pekko `EventSourcedBehavior`, provided by a `TypedAggregateRootManager`. The name of its `entityTypeKey` is the aggregate type. The gateway passes the persistence id to `behavior`; build the behavior with `AggregateBehavior`, which persists under that id and tags the events with the aggregate type:

```scala
class CargoManager extends TypedAggregateRootManager[CargoCommand] {

  override def entityTypeKey: EntityTypeKey[CargoCommand] = EntityTypeKey[CargoCommand](Cargo.Type.name)

  override def behavior(persistenceId: PersistenceId): Behavior[CargoCommand] =
    AggregateBehavior.withEnforcedReplies(persistenceId, CargoState.empty, commandHandler, eventHandler)
}
```

The result is a plain `EventSourcedBehavior`, so `.snapshotWhen`, `.withRetention`, `.receiveSignal`, `.eventAdapter` etc. can be chained. A behavior built by hand must use the given persistence id and tag every event with `aggregateType.name`. Inside the behavior, the aggregate id is `AggregateType.aggregateIdOf(persistenceId)`.

The typed gateway starts aggregates on demand and stops them after `aggregateMayIdleFor` (6 minutes by default). Its `ask` takes a function that builds the command around the reply address:

```scala
val gateway = new DefaultTypedCommandGateway[CargoCommand](system, new CargoManager())

gateway.ask[CargoResponse](cargoId, replyTo => PlanCargo(cargoId, trackingId, routeSpecification, replyTo))
gateway.tell(SpecifyNewRoute(cargoId, routeSpecification))
```

## Event materializers

An event materializer reads the events of one aggregate type and builds a query store. A replayable materializer first replays the stored events (`handleReplayEvent`) and then keeps listening for new ones (`handleEvent`):

```scala
class CargoOverview(system: ActorSystem)
    extends AbstractReplayableEventMaterializer(system)
    with ReadJournalOffsetStore {

  override val logger: Logger         = Logger(LoggerFactory.getLogger(classOf[CargoOverview]))
  override val aggregateType          = Cargo.Type
  override val matMappingName: String = "cargo-overview" // identifies the stored offset

  override def handleEvent(evt: Any): Future[Done] = evt match {
    case CargoPlanned(id, trackingId, _) => repository.insert(id, trackingId)
    case _                               => Future.successful(Done)
  }

  override def handleReplayEvent(evt: Any): Future[Done] = handleEvent(evt)
}
```

To react to the events of several aggregate types, use [combined logic](#combined-logic).

Start all materializers together. `startUp` replays every replayable materializer first, then starts listening from where each replay ended:

```scala
new EventMaterializers(List(new CargoOverview(system), new DeliveryPlanning(system))).startUp(keepListenersRunning = true)
```

Things to know:
- **Offsets.** A materializer stores the offset of the last handled event, so it resumes after a restart. `ReadJournalOffsetStore` keeps the offsets in the same database as the journal. `OffsetStoreProvider` uses an in-memory or LMDB store, chosen by `bounded.eventmaterializers.offsetstore.type`. With `withPartialReplay = false` a materializer stores no offset and replays all events at every start.
- **At-least-once delivery.** After a failure or a restart an event can be delivered again, so write handlers that are idempotent. A failing stream is restarted with backoff (`bounded.eventmaterializers.restart`).
- **Filtering.** A `MaterializerEventFilter` skips events at runtime.
- **One node.** `EventMaterializers` runs its materializers on the node that starts it. In a cluster, run materializers that write to a shared store with `ClusteredEventMaterializers` (see [Running in a cluster](#running-in-a-cluster)).

## Combined logic

Combined logic is business logic that spans aggregates: it reacts to the events of several aggregate types and usually sends commands to other aggregates through a command gateway. Extend `CombinedLogicEventMaterializer`, list the `aggregateTypes` to read and handle the events in `handleEvent`:

```scala
class DeliveryPlanning(
  system: ActorSystem,
  cargoGateway: TypedCommandGateway[CargoCommand],
  orderGateway: TypedCommandGateway[OrderCommand]
) extends CombinedLogicEventMaterializer(system)
    with ReadJournalOffsetStore {

  override val logger: Logger         = Logger(LoggerFactory.getLogger(classOf[DeliveryPlanning]))
  override val aggregateTypes         = List(Order.Type, Cargo.Type)
  override val matMappingName: String = "delivery-planning" // identifies the stored offsets

  override def handleEvent(evt: Any): Future[Done] = evt match {
    case OrderConfirmed(orderId, cargoId, route) =>
      cargoGateway.tell(PlanCargo(cargoId, orderId, route)).map(_ => Done)
    case CargoDelivered(cargoId, orderId) =>
      orderGateway.tell(CompleteOrder(orderId)).map(_ => Done)
    case _ => Future.successful(Done)
  }
}
```

Start it with the materializers: `new EventMaterializers(List(new CargoOverview(system), new DeliveryPlanning(system, cargoGateway, orderGateway))).startUp(keepListenersRunning = true)`.

How it differs from a materializer:
- **Only new events.** Combined logic has no replay phase; at its first start it reads every stored event of its types, after that it resumes from its offsets. When it is added to a system with existing events and must not act on them, set its offsets to the end first with `AggregateTypeMigration.startCombinedLogicAtEnd(offsetStore, "delivery-planning", Seq(Order.Type, Cargo.Type))`.
- **An offset per aggregate type.** The events of the types are merged, and the stored offset holds a position per type (`TagOffsets`), so a restart skips no events of a type that lags. Adding a type later reads that type from the beginning; a type that is removed is dropped from the offset. On the JDBC offset store the offsets of all types must fit in 4000 characters.
- **No order across types.** Events of one aggregate arrive in order, but events of different types can interleave in any order. Keep the state the logic needs in an aggregate (a process manager) it sends commands to, not in the combined logic itself.
- **Side effects happen at least once.** After a failure or a restart an event can be handled again, so make the commands it sends idempotent (for example by deriving ids from the event).
- **Once in a cluster.** Run it with `ClusteredEventMaterializers`, otherwise every node sends the commands (see [Running in a cluster](#running-in-a-cluster)).

## Running in a cluster

On one node nothing changes. In a cluster (`pekko.actor.provider = cluster`), each aggregate and each read model with a shared store must run on one node at a time.

**Aggregates.** Use `ShardedTypedCommandGateway` instead of `DefaultTypedCommandGateway`. It has the same `ask` / `tell` / `shutdown`, and runs the aggregates with Cluster Sharding: commands for one aggregate reach the same actor, wherever they are sent from. Create it on every node that handles commands:

```scala
val gateway = new ShardedTypedCommandGateway[CargoCommand](system, new CargoManager(), aggregateMayIdleFor = 6.minutes)
```

`shutdown()` hands the node's aggregates over to the other nodes. The classic gateways (`DefaultCommandGateway`, `RouterCommandGateway`) stay node-local: run classic aggregates on one node, or move them to typed aggregates.

**Materializers and combined logic.** `ClusteredEventMaterializers` runs each materializer on one node, spread over the cluster with a `ShardedDaemonProcess`. When a node leaves, its materializers resume on another node from their stored offsets, so the offset store must be shared by the nodes (`ReadJournalOffsetStore`). Every node starts it with the same materializers:

```scala
new ClusteredEventMaterializers(system, List(new CargoOverview(classicSystem), new DeliveryPlanning(classicSystem, cargoGateway, orderGateway))).start()
```

Its materializers implement `StoppableResumable`, which `AbstractEventMaterializer`, `AbstractReplayableEventMaterializer` and `CombinedLogicEventMaterializer` do. Each replays and then listens on its own; it does not wait for the replays of the others. A read model that every node keeps for itself (in memory, or with the in-memory or LMDB offset store) stays on `EventMaterializers`, on every node.

**Serialization.** Commands, replies and events cross nodes, so each needs a serializer binding; turn Java serialization off (`pekko.actor.allow-java-serialization = off`). Bounded serializes its own `Ok`, `Ko`, `PersistenceFailed`, `PersistenceRejected` and `UnexpectedCommand`, and writes the events, failures and commands inside them with the serializers you bind to your types. The cause of a `PersistenceFailed` arrives on another node as a `RemoteFailureCause` with the original class name and message.

Delivery to materializers stays at-least-once: when a materializer moves, the events after its last saved offset are handled again. Use a split brain resolver (`pekko.cluster.downing-provider-class`): without one, both halves of a split cluster run every aggregate and every materializer.

## Pekko Projections

`bounded-pekko-projection` runs a Bounded read model as a [Pekko Projection](https://pekko.apache.org/docs/pekko-projection/current/), next to the event materializers. Use it when a read model needs exactly-once results: the handler's writes and the offset are committed in one transaction. Describe the read model with an `EventProjection` and pick a handler for the projection style:

```scala
object CargoReport extends EventProjection {
  override val projectionName = "cargo-report"
  override val aggregateType  = Cargo.Type
}

val projection = JdbcProjection
  .exactlyOnce(
    CargoReport.projectionId,
    BoundedProjections.eventsByAggregateType(system, CargoReport.aggregateType),
    () => new MyJdbcSession(dataSource.getConnection),
    () => BoundedJdbcHandler.byTag(CargoReport) { (session: MyJdbcSession, event) => /* write in the session */ }
  )
  .withStatusObserver(BoundedProjections.publishEventProcessed(system, CargoReport))

BoundedProjections.runInCluster(system, "read-models", List(projection)) // or runLocally(system, projection)
```

Every handler has `byTag` (for `eventsByAggregateType`) and `bySlices` (for `eventsByAggregateTypeSlices`).

| Projection | Handler | Source |
|---|---|---|
| `JdbcProjection.exactlyOnce` (`pekko-projection-jdbc`) | `BoundedJdbcHandler` | `eventsByAggregateType` |
| `SlickProjection.exactlyOnce` (`pekko-projection-slick`) | `BoundedSlickHandler` | `eventsByAggregateType` |
| `R2dbcProjection.exactlyOnce` (`pekko-projection-r2dbc`) | `BoundedR2dbcHandler` | `eventsByAggregateTypeSlices` (R2DBC journal) |
| any `atLeastOnce`, also `CassandraProjection` | `BoundedHandler` | either |

- Add the projection module of your store yourself; `bounded-pekko-projection` does not pull them in.
- The `MaterializerEventFilter` of an `EventProjection` works as for materializers: filtered events skip the handler, and their offset is stored. `publishEventProcessed` publishes `EventProcessed` when `bounded.eventmaterializers.publish` is on (for exactly once: after the commit).
- A projection keeps its offset in the tables of Pekko Projections, not in Bounded's offset stores. A read model moved from a materializer to a projection starts from the first event. There is no in-memory or LMDB equivalent.
- Replay-then-stop (`startUp(keepListenersRunning = false)`), `handleReplayEvent` and combined logic over several types stay on the event materializers.
- Projections run once in a cluster with `runInCluster`, which uses a `ShardedDaemonProcess` with one process per projection.

## Operating notes

- **Handlers are at-least-once.** Materializers, combined logic and at-least-once projections can see an event again after a failure, a restart or a move to another node. Make them idempotent, or use an exactly-once projection.
- **Serializers.** A serializer must not throw on an event it stored before, and must give new fields a default: one event it cannot read stops the recovery of its aggregate and the materializers that read it. Bounded ships no serializer for your types.
- **Tags.** Bounded tags events with their aggregate type. Do not add a tag per aggregate (`tags(event)` returning an id): each tag is a query, and the tag table grows with every aggregate.
- **JDBC journal.** Queries by tag filter `event_tag` on `tag`, but its key is `(event_id, tag)`. Add an index: `CREATE INDEX event_tag_tag_idx ON event_tag (tag, event_id);`.
- **Combined logic.** Its offset holds a position per aggregate type, and on the JDBC offset store all of them must fit in 4000 characters: keep the number of types small.
- **LMDB offset store.** lmdbjava needs `--add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED` on Java 17 and later.

## Journals

Bounded works with the Pekko persistence plugins below, chosen with `pekko.persistence.journal.plugin`. Materializers need the query side of the journal, and read an aggregate type in a way that depends on it:

| Journal | How an aggregate type is read |
|---|---|
| R2DBC (`pekko-persistence-r2dbc`, e.g. Postgres) | by entity type: the part of the persistence id before `\|` |
| JDBC, Cassandra, LevelDB, `inmemory-journal` | by tag: every event is tagged with its aggregate type |

On R2DBC, classic aggregates only wrap an event in `Tagged` when `tags(event)` gives extra tags, so write-side event adapters see the events. On the tag-based journals every event is wrapped, and an event adapter bound to an event class does not see it. Typed aggregates always tag; use `.eventAdapter` on the behavior instead.

The R2DBC journal re-reads a recent window (`pekko.persistence.r2dbc.query.backtracking.window` plus `behind-current-time`) to catch events committed late. Bounded drops the duplicates this produces, but after a restart the events of that window before the stored offset are handled again.

For tests, Pekko's bundled `pekko.persistence.journal.inmem` works for aggregates, but it has no query side. Anything with materializers needs `inmemory-journal` (`io.github.alstanchev:pekko-persistence-inmemory`, included) or a real database.

## Testing

`bounded-test` has helpers to test aggregates and projections with given-when-then:

```scala
val cargo = TestableAggregateRoot
  .givenEvents[Cargo, CargoState](cargoCreator, cargoId, CargoPlanned(cargoId, trackingId, route))
  .when(SpecifyNewRoute(cargoId, newRoute))

cargo.events shouldBe List(NewRouteSpecified(cargoId, newRoute))
```

- `TestableAggregateRoot` (classic and `typed`): start an aggregate from events, send a command, check the events, the state or the failure.
- `TestableProjection.givenEvents(events, Cargo.Type)`: store events for an aggregate type, then `startProjection(materializer)` and `addEvent(...)`.
- `TestTypedCommandGateway`: a typed gateway for tests.

## Upgrading from 1.0

Bounded 1.0 stored streams without an aggregate type (or with a `persistenceIdPrefix`), so materializers read them by a tag of their own. From this version every persistence id is `type|id`, and materializers name an `aggregateType` instead of a `tagName`. The API changes:
- Classic aggregates: `persistenceIdPrefix` is replaced by the abstract `aggregateType`.
- Typed aggregates: `behavior(id: String)` became `behavior(persistenceId: PersistenceId)`; build it with `AggregateBehavior`.
- Materializers and combined logic: `tagName` / `tagNames` became `aggregateType` / `aggregateTypes`.

An existing journal is converted with `AggregateTypeMigration`. It runs in the application's own actor system, with the production journal config, while the application is stopped:

```scala
val migration = new AggregateTypeMigration(
  system,
  AggregateTypeMigration.byPrefix("cargo-" -> Cargo.Type, "Order|" -> Order.Type)
)

for {
  plan     <- migration.dryRun()                    // check: targets, splits, conflicts, untagged streams
  migrated <- migration.migrate()                   // resumable copy; check migrated.verified
  _        <- migration.deleteMigrated(migrated)    // delete the old streams and their snapshots
  _        <- migration.startCombinedLogicAtEnd(offsetStore, "delivery-planning", Seq(Cargo.Type, Order.Type))
  _        <- migration.startAtEnd(offsetStore, "cargo-overview", Cargo.Type) // keeps its data
  _        <- migration.resetOffsets(offsetStore, Seq("order-view"))         // rebuilt from scratch
} yield ()
```

- **The rule** maps an old persistence id and event to a `MigrationTarget(type, id)`. `byPrefix` and `byEvent` cover the common cases; `withTags` adds the extra tags of `tags(event)`. Events of one old stream with different targets split it.
- **Safety.** `migrate()` copies nothing when two old streams share a target, or a target is itself migrated. When it is run again it checks what a target already holds. `verify` compares the counts.
- **Materializers.** The copies get new offsets. Combined logic must start at the end (`startCombinedLogicAtEnd`) so it does not repeat its side effects. On R2DBC, `startAtEnd` / `startCombinedLogicAtEnd` wait for the backtracking window (about 2 minutes by default) before they return.
- **Cassandra** keeps deleted events in its tag views; `oldStreamsReadableByType` lists the old streams still readable by type.

See the scaladoc of `AggregateTypeMigration` for the full order of operations.

# Open source

The code is  [Apache2 licensed](https://www.apache.org/licenses/LICENSE-2.0)  and available in the  [Cafienne Bounded Framework Github Repository](https://github.com/cafienne/bounded-framework) .

# Releasing

Bounded is built for Scala 3 with sbt 2 and published to Maven Central (`io.cafienne.bounded` : `bounded-core_3`, `bounded-test_3`, `bounded-pekko-http_3`) using sbt's built-in Sonatype Central support.

sbt 2 starts a thin client that takes all its arguments as one command line, so several commands go in one quoted argument separated by `;` (`sbt "publishSigned; sonaRelease"`); `sbt publishSigned sonaRelease` fails.

## Prerequisites

- A Sonatype Central user token for the `io.cafienne` namespace, stored in `~/.sbt/sonatype_central_credentials`:
  ```
  host=central.sonatype.com
  user=<token username>
  password=<token password>
  ```
- A GPG key that is published to a public keyserver; it is used by sbt-pgp to sign the artifacts.

## Versioning

There is no version file: the version is derived from the git tag. Release tags have the form `vX.Y.Z`.

- A build on a commit that carries a tag `vX.Y.Z`, without uncommitted changes, has version `X.Y.Z`.
- Any other build (commits after the last tag, or local changes) has version `X.Y.(Z+1)-SNAPSHOT`.

Check which version will be published with `sbt "show boundedCore/version"`.

## Publishing a release

```bash
git checkout main && git pull
git tag v1.0.0
git push origin v1.0.0
sbt "show boundedCore/version"   # must print 1.0.0, without -SNAPSHOT
sbt "publishSigned; sonaRelease"
```

Uncommitted changes to tracked files turn the build into a snapshot (untracked files do not), so commit or stash them before publishing a release.

`publishSigned` signs the artifacts and stages them locally; `sonaRelease` uploads the bundle to Sonatype Central and releases it to Maven Central. Use `sonaUpload` instead of `sonaRelease` to upload only, and publish the deployment manually from the [Central Portal](https://central.sonatype.com/publishing/deployments).

Afterwards, create a GitHub release for the tag with the release notes.

## Publishing a snapshot

On any commit that is not a release tag, `sbt publishSigned` publishes the `-SNAPSHOT` version directly to the [Central snapshots repository](https://central.sonatype.com/repository/maven-snapshots/).

# Release notes

Are found at [https://github.com/cafienne/bounded-framework/releases](https://github.com/cafienne/bounded-framework/releases)

