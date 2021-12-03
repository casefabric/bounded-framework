## Domain driven design framework for Scala and Akka

Bounded is a framework that supports the Domain Driven Design approach. It enables you to leverage Scala and Akka technologies in a way that gives you a structured DDD based application.
[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=cafienne_bounded-framework&metric=sqale_rating)](https://sonarcloud.io/summary/new_code?id=cafienne_bounded-framework)


```scala
class Cargo(cargoId: AggregateRootId) 
   extends AggregateRootActor 
   with AggregateStateCreator 
   with ActorLogging {

  override def aggregateId: 
              AggregateRootId = cargoId

  override def handleCommand(
            command: DomainCommand, 
            currentState: AggregateState)
          : Reply = {

    command match {
      case cmd: PlanCargo =>
        Ok(Seq(
          CargoPlanned(cmd.metaData, 
                    cmd.cargoId, 
                    cmd.trackingId, 
                    cmd.routeSpecification)))
      case cmd: SpecifyNewRoute =>
        Ok(Seq(
          NewRouteSpecified(
                cmd.metaData, 
                cmd.cargoId, 
                cmd.routeSpecification)))
      case other => 
        Ko(UnexpectedCommand(other))
    }
  }
}
```
        

The example shows the Cargo example as described in the book of Eric Evans. Bounded takes care of the persistence of the aggregate root via an event sourced mechanism based on akka persistence. Next to that, it takes care of handling the internal state of the aggregate root.

```scala
case class CargoAggregateState(
        trackingId: TrackingId, 
        routeSpecification: RouteSpecification)
    extends AggregateState {

    override def update(evt: DomainEvent)
                          : AggregateState = {
      evt match {
        case CargoPlanned(meta, 
              cargoId, 
              trackingId, 
              routeSpecification) =>
          CargoAggregateState(trackingId, 
                        routeSpecification)
        case NewRouteSpecified(meta, 
              cargoId, 
              routeSpecification) =>
          this.copy(routeSpecification
                     = routeSpecification)
        case other => this
      }
    }
  } 
```
        

## Event sourcing

Bounded makes use of a Event Sourcing approach. All logic is expressed in events that allows you to extend your application based on those events. The resumable projections are built with the reactive technology akka streams. Using an event store also means that you have the full history of the application at hand, allowing to create new historic based views later in time.

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

# Release notes

Are found at [https://github.com/cafienne/bounded-framework/releases](https://github.com/cafienne/bounded-framework/releases)

