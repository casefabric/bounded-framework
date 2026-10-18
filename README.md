## Domain driven design framework for Scala and Pekko

Bounded is a framework that supports the Domain Driven Design approach. It enables you to leverage Scala and Apache Pekko technologies in a way that gives you a structured DDD based application.
[![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=casefabric_bounded-framework&metric=reliability_rating)](https://sonarcloud.io/summary/new_code?id=casefabric_bounded-framework)

```scala
class Cargo(cargoId: String) extends AggregateRootActor[CargoState] {

  override def aggregateId: String = cargoId

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

  // Event materializers read the events by tag
  override def tags(event: DomainEvent): Set[String] = Set("cargo")
}
```

The example shows the Cargo example as described in the book of Eric Evans. Bounded takes care of the persistence of the aggregate root via an event sourced mechanism based on Pekko persistence: the events of an `Ok` reply are stored before the reply is sent. Next to that, it takes care of handling the internal state of the aggregate root. `newState` creates the state from the first event, and the state then evolves with every next event:

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

A command gateway starts the aggregate root for a command when it is not running, validates the command, delivers it and stops aggregate roots that are idle:

```scala
val cargoCreator = new AggregateRootCreator {
  override def props(cargoId: String): Props = Props(new Cargo(cargoId))
}
val gateway = new RouterCommandGateway(system, idleTime = 10.minutes, cargoCreator)

implicit val planCargoValidator: ValidateableCommand[PlanCargo] = cmd => Future.successful(cmd)

gateway.sendAndAsk(PlanCargo(cargoId, trackingId, routeSpecification)) // Future of the Reply: Ok(events) or Ko(failure)
```

## Event sourcing

Bounded makes use of a Event Sourcing approach. All logic is expressed in events that allows you to extend your application based on those events. The resumable projections are built with the reactive technology Pekko streams. Using an event store also means that you have the full history of the application at hand, allowing to create new historic based views later in time.

## Command Query Responsibility Segregation (CQRS)

## Architecture

The bounded framework enables a CQRS style Domain Driven Design application.

```
                 ┌─────────┐   ┌──────────────────────┐                         
                 │         │   │      DOMAIN          │                         
┌───────┐        │ Command │   │        ┌───────┐     │                         
│       │        │ Routing │   │        │       │     │                         
│       │────────▶         ├───┼────────▶       │─────┼────────────────┐        
│       │        │         │   │        │ AR    │     │                │        
│GRAPHQL│        │         │   │        └───────┘     │                ▼        
│ API   │        │         │   │                      │       ┌────────────────┐
│       │        └─────────┘   │                      │       │   PERSISTENCE  │
│       │                      │                      │       │                │
│       │                      │                      │       │                │
│       │◀─────────┐           └──────────────────────┘       │   EVENT        │
│       │          │                                          │   STORE        │
│       │          │                                          │                │
│       │          │           ┌──────────────────────┐       │                │
└───────┘          │           │   Materializers      │       │                │
                   │           │┌─────────┬─────────┐ │       │                │
                   │           ││         │         | │       └────────────────┘
                   │           ││ Item    │ Item    │ │                │        
                   └───────────┤│ Query   │ Writer  │◀┼────────────────┘        
                               ││Interface│         │ │                         
                               │└─────────┴─────────┘ │                         
                               │                      │                         
                               │                      │                         
                               │                      │                         
                               └──────────────────────┘  
AR = Aggregate Root                                          
```

The framework allows you to write a CQRS based application that seperates the intent by issueing commands to your application and thereafter make use of the data via projection created query stores. Resumable projections allow to create any kind of query store that is applicable for your application. The projections listen to the events created and therafter create their own records in a store that may be a relational database store, elastic search, a cache or any kind of other storage. The tracking of the projections ensures that no events are forgotten even when the application is upgrading. When your projections need updates, it is always possible to replay the full store.

# Open source

The code is  [Apache2 licensed](https://www.apache.org/licenses/LICENSE-2.0)  and available in the  [Cafienne Bounded Framework Github Repository](https://github.com/cafienne/bounded-framework) .

# Releasing

Bounded is published to Maven Central (`io.cafienne.bounded` : `bounded-core`, `bounded-test`, `bounded-pekko-http`) using sbt's built-in Sonatype Central support.

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
git tag v0.4.0
git push origin v0.4.0
sbt publishSigned sonaRelease
```

`publishSigned` signs the artifacts and stages them locally; `sonaRelease` uploads the bundle to Sonatype Central and releases it to Maven Central. Use `sonaUpload` instead of `sonaRelease` to upload only, and publish the deployment manually from the [Central Portal](https://central.sonatype.com/publishing/deployments).

Afterwards, create a GitHub release for the tag with the release notes.

## Publishing a snapshot

On any commit that is not a release tag, `sbt publishSigned` publishes the `-SNAPSHOT` version directly to the [Central snapshots repository](https://central.sonatype.com/repository/maven-snapshots/).

# Release notes

Are found at [https://github.com/cafienne/bounded-framework/releases](https://github.com/cafienne/bounded-framework/releases)

