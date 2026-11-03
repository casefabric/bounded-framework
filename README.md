## Domain driven design framework for Scala and Pekko

Bounded is a framework that supports the Domain Driven Design approach. It enables you to leverage Scala and Apache Pekko technologies in a way that gives you a structured DDD based application, built on event sourcing and CQRS.
[![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=casefabric_bounded-framework&metric=reliability_rating)](https://sonarcloud.io/summary/new_code?id=casefabric_bounded-framework)

The flow through an application is:

```
command → command gateway → aggregate root (stores events) → event journal → event materializers → query stores
```

- An **aggregate root** handles the commands for one aggregate and stores the resulting events.
- A **command gateway** starts the aggregate root a command is for, validates the command and delivers it.
- **Event materializers** (projections) read the stored events of an aggregate type and build query stores from them; **sagas** react to the events of several aggregate types.

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
  "io.cafienne.bounded" %% "bounded-pekko-http" % boundedVersion         // optional: Pekko HTTP helpers
)
```

All configuration uses Pekko's `pekko.` keys; `akka.` keys are silently ignored.

## Aggregate types

Every aggregate has an `AggregateType`. Define it once and use it on both the write side (the aggregate) and the read side (materializers and sagas):

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

## Event materializers and sagas

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

A saga extends `SagaEventMaterializer`, lists its `aggregateTypes` and only handles new events; it keeps an offset per type.

Start all materializers together. `startUp` replays every replayable materializer first, then starts listening from where each replay ended:

```scala
new EventMaterializers(List(new CargoOverview(system), new DeliverySaga(system))).startUp(keepListenersRunning = true)
```

Things to know:
- **Offsets.** A materializer stores the offset of the last handled event, so it resumes after a restart. `ReadJournalOffsetStore` keeps the offsets in the same database as the journal. `OffsetStoreProvider` uses an in-memory or LMDB store, chosen by `bounded.eventmaterializers.offsetstore.type`. With `withPartialReplay = false` a materializer stores no offset and replays all events at every start.
- **At-least-once delivery.** After a failure or a restart an event can be delivered again, so write handlers that are idempotent. A failing stream is restarted with backoff (`bounded.eventmaterializers.restart`).
- **Filtering.** A `MaterializerEventFilter` skips events at runtime.
- **One node only.** The gateways and materializers are node-local: two nodes would run two aggregate roots on one stream, and every node would run every materializer. Run Bounded on a single node.

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
- Materializers and sagas: `tagName` / `tagNames` became `aggregateType` / `aggregateTypes`.

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
  _        <- migration.startSagaAtEnd(offsetStore, "delivery-saga", Seq(Cargo.Type, Order.Type))
  _        <- migration.startAtEnd(offsetStore, "cargo-overview", Cargo.Type) // keeps its data
  _        <- migration.resetOffsets(offsetStore, Seq("order-view"))         // rebuilt from scratch
} yield ()
```

- **The rule** maps an old persistence id and event to a `MigrationTarget(type, id)`. `byPrefix` and `byEvent` cover the common cases; `withTags` adds the extra tags of `tags(event)`. Events of one old stream with different targets split it.
- **Safety.** `migrate()` copies nothing when two old streams share a target, or a target is itself migrated. When it is run again it checks what a target already holds. `verify` compares the counts.
- **Materializers.** The copies get new offsets. A saga must start at the end (`startSagaAtEnd`) so it does not repeat its side effects. On R2DBC, `startAtEnd` / `startSagaAtEnd` wait for the backtracking window (about 2 minutes by default) before they return.
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

