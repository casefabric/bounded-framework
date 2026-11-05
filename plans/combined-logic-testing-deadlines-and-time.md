# Combined logic testing, deadlines, scheduling and controllable time

Nov 2, 2026 · @Olger Warnier · based on the `TestableCombinedLogic` stub removed from `bounded-test` and on case-engine's timer service · revised Nov 4, 2026 for aggregate types (#79), clustering and `bounded-pekko-projection`

## Summary

`bounded-test` used to ship an empty `TestableSaga` class whose comments were copied from Axon Framework's `SagaTestFixture`. It was never implemented and has been removed. This plan covers what that fixture stood for, in two parts:

- **Combined logic testing (test helpers only).** Bounded can run a combined logic (`CombinedLogicEventMaterializer`), but has no helper to test one: `TestableProjection` only accepts an `AbstractEventMaterializer`, and there is no command gateway that records what a combined logic sends. Items S1–S3 fix that without changing the framework.
- **Deadlines, scheduling and time (framework features).** Bounded has no way to make something happen later that survives a restart, and nothing in it reads the time through a replaceable clock. Pekko timers inside an aggregate are lost when the aggregate is passivated (after `aggregateMayIdleFor`, 6 minutes by default) or the node restarts. Items S4–S10 add a clock abstraction, durable deadlines owned by aggregates, a scheduler for callers without an event stream (combined logic, services), and test support that moves time forward.

The deadline design follows case-engine's timer service, which has been in production for years: the owner persists "deadline set" and "deadline cleared" events, and a separate service reads them, keeps the pending deadlines, and sends a command back to the owner when one expires. case-engine finds these events by a tag; Bounded finds them by the aggregate types of their owners (design decision 2), because the R2DBC journal has no queries by tag.

**Status:** nothing is implemented yet. Everything this plan needed from [event-sourcing-projection-fixes](event-sourcing-projection-fixes.md) is done: restart and offset handling (5, 7), offset encoding and stores (4, 10), the R2DBC journal by aggregate type (21), serialization of replies (17) and running in a cluster (15, 22).

## What the Axon fixture had, and where Bounded stands

| Axon fixture element | Purpose | Bounded today | Item |
| --- | --- | --- | --- |
| `sagaType`, `handlerDefinition` | The combined logic under test | `CombinedLogicEventMaterializer` with `aggregateTypes` and `handleEvent(evt: Any): Future[Done]` | — |
| `sagaStore` (`InMemorySagaStore`) | Saga instances and their state | No instances per business transaction: a combined logic is one stream handler that keeps its own state; Bounded only stores its position per aggregate type (`TagOffsets`, keyed by type name) in an offset store | — (by design) |
| `eventBus`, `givenAPublished`, `whenPublishingA`, `givenAggregate(id).published`, `whenAggregate(id).publishes`, `andThenAPublished`, `andThenAggregate` | Feed events to the combined logic | Events reach a combined logic from the journal by aggregate type (by tag, or by entity type on R2DBC). `TestableProjection.givenEvents(events, aggregateType)` / `addEvent(s)` write them, but for one aggregate type only, only for an `AbstractEventMaterializer`, and every event of one call is stored under the id of the first (`aggregateType.persistenceId(evt.head.id)`) | S1, S3 |
| `givenNoPriorActivity` | Start from an empty history | `TestableProjection.givenEvents(Seq.empty)` clears the in-memory offset store, not the journal | S3 |
| `commandBus` (`RecordingCommandBus`), `DefaultCommandGateway` | Record the commands the combined logic sends | `CommandGateway` and `TypedCommandGateway` exist (typed: `DefaultTypedCommandGateway`, `ShardedTypedCommandGateway`); no recording version. `TestTypedCommandGateway` spawns real aggregates | S2 |
| `expectDispatchedCommands`, `expectNoDispatchedCommands` | Assert on sent commands | None | S2, S3 |
| `deadlineManager` (`StubDeadlineManager`), `expectScheduledDeadline`, `expectTriggeredDeadlines` | Deadlines that call back into the combined logic or aggregate | None. Aggregates can use Pekko timers, which are not durable | S5, S6, S9 |
| `eventScheduler` (`StubEventScheduler`), `expectScheduledEvent` | Publish an event at a later moment | None | S7 (as scheduled commands, see design decision 3) |
| `givenCurrentTime`, `whenTimeElapses`, `whenTimeAdvancesTo`, `andThenTimeElapses`, `andThenTimeAdvancesTo` | Control time in a test | None. Pekko's `ManualTime` controls the actor scheduler (used in `TypedCommandGatewaySpec`), but not `Instant.now()` | S4, S9 |
| `fixtureExecutionResult`, `fieldFilters` | Collect results; ignore fields when comparing | None. ScalaTest matchers on case classes cover most of this; field filters are not planned | S3 |

## Design decision 1: one clock, two halves of time

Time in a test has two halves that must move together:

- **Reading the time.** Code that decides whether a deadline is due, or stamps metadata, reads a wall clock. Pekko's `ManualTime` does not change `Instant.now()`.
- **Waking up.** Code waits for a moment through the Pekko scheduler. `ManualTime` (which installs `org.apache.pekko.testkit.ExplicitlyTriggeredScheduler`) controls that, but only when it is configured at actor system creation.

Decision:

- Add a Pekko extension `BoundedTime(system).clock: java.time.Clock`, defaulting to `Clock.systemUTC()`. Bounded's runtime reads the time only through it (today only `AggregateTypeMigration`, an offline tool, calls `Instant.now()`, to wait out R2DBC timestamps; it can stay on the system clock; the deadline service will be the first user). Users are encouraged to read it in command handlers too, so their metadata timestamps follow test time.
- Configure it with `bounded.time.clock = "system"` (default) or `"manual"`. With `"manual"` the extension holds a `ManualClock` (a mutable `Clock` starting at a configurable instant, or now).
- In `bounded-test`, a `TestTime` helper moves both halves: `advance(d)` first moves the `ManualClock`, then calls `ManualTime.timePasses(d)`, so scheduled tasks that fire see the new time. It fails fast with a clear message when the system was not created with `ManualTime.config` and the manual clock.
- Nothing global: no `System.currentTimeMillis` replacement, no static clock. Two actor systems in one JVM can run on different clocks.

## Design decision 2: deadlines are events of their owner, delivered by a service

A deadline must survive passivation, restarts and redeploys, so it cannot live in an actor's memory. Following case-engine:

- **Setting and clearing.** The owner (an aggregate) persists a `DeadlineSet` event and, when the deadline is no longer needed or has been handled, a `DeadlineCleared` event. Both are traits in the Bounded protocol that the user's events extend:

  ```scala
  trait DeadlineSet extends DomainEvent {
    def deadlineId: String   // unique per owner; setting the same id again moves the deadline
    def dueAt: Instant
    def name: String         // what the deadline is for, used by the owner when it expires
  }
  trait DeadlineCleared extends DomainEvent { def deadlineId: String }
  ```

  The deadline is part of the aggregate's own history, so it is set if and only if the command that set it succeeded, and replaying the aggregate shows it.
- **Finding them: by aggregate type, not by tag.** The first version of this plan had the owner tag these events with a deadline tag. That no longer fits: the R2DBC journal has no queries by tag, the classic `tags(event)` hook only works on tag-based journals, and `AggregateBehavior` tags typed events with the aggregate type only. Instead the service is given the aggregate types that own deadlines, `DeadlineService(aggregateTypes, deliver, ...)`, and reads them through `AggregateTypeReadJournal` like a combined logic, keeping only `DeadlineSet` / `DeadlineCleared` events. This works on every journal and leaves nothing for the user to tag. The cost is reading every event of those types; S8 keeps start-up short by storing what is pending.
- **The service.** A `DeadlineService` is a materializer over those types, built like `CombinedLogicEventMaterializer` on `EventStreamHandling` (restart and offset handling from items 5 and 7 of [event-sourcing-projection-fixes](event-sourcing-projection-fixes.md), an offset per type as `TagOffsets`, at-least-once delivery from the journal, re-delivered backtracking on R2DBC). It keeps the pending deadlines and schedules each one with the Pekko scheduler. It implements `StoppableResumable`, so `ClusteredEventMaterializers` can run it once per cluster (S10).
- **Delivery.** Bounded cannot build the user's command, so the service is given a function: `deliver: Deadline => Future[Any]`, for example `d => gateway.ask(d.ownerId, ref => OrderDeadlineReached(d.deadlineId, d.name, ref))`. A failed or timed-out delivery is retried with backoff (case-engine: every 10 s, 10 times; here configurable under `bounded.deadlines.retry`). A deadline stays pending until its `DeadlineCleared` arrives, so the owner must persist `DeadlineCleared` when it handles the expiry. Delivery is at least once: the owner's handler must ignore a deadline it already handled or cleared.
- **Overdue deadlines** (due while the service was down) are delivered right after start.
- **Window.** The Pekko scheduler rejects delays beyond its maximum (about 248 days with the default 10 ms tick), and holding years of deadlines as scheduled tasks wastes memory. The service only schedules deadlines due within a window (`bounded.deadlines.window`, default 1 day) and re-reads the pending deadlines every `bounded.deadlines.interval` (default 1 hour), like case-engine's `TimerStorageReader`.
- **Where pending deadlines live.** Start simple (S6): on start, replay the owners' aggregate types from the beginning into memory. That needs no new store and is correct, but start-up time grows with history. S8 adds a `DeadlineStore` (in-memory, JDBC, R2DBC, Cassandra) that holds the pending deadlines and the service's offset in the same transaction, so start-up only reads what is due.
- **Not inside the aggregate.** Bounded does not schedule Pekko timers inside aggregates for this. Those are fine for short, best-effort timeouts and stay available to users, but they are lost on passivation.

## Design decision 3: scheduled commands instead of scheduled events

Axon's `EventScheduler` publishes an event later. In Bounded every event belongs to an aggregate's stream, so "publish an event later" means "send a command later to the aggregate that will emit it". Combined logic also need deadlines, but a Bounded combined logic has no persistence id to persist `DeadlineSet` under.

Decision: a `CommandScheduler` for callers without their own stream. It is an event-sourced aggregate of its own aggregate type (`AggregateType("bounded-command-scheduler")`, configurable, so its persistence id is `bounded-command-scheduler|<name>`, built with `AggregateBehavior`) whose events (`CommandScheduled`, `ScheduledCommandCancelled`, `ScheduledCommandDelivered`) are `DeadlineSet` / `DeadlineCleared`. The `DeadlineService` reads that type along with the user's types, delivers them with the same retry, window and store, and calls the scheduler back, which persists `ScheduledCommandDelivered`. API:

```scala
trait CommandScheduler[T <: DomainCommand] {
  def schedule(id: String, at: Instant, command: T): Future[Done]   // same id again replaces it
  def cancel(id: String): Future[Done]
}
```

The command is persisted, so it must have a serializer binding (Java serialization is off in a cluster) and must not carry an `ActorRef`; typed commands that need a `replyTo` are scheduled through a command factory registered by name instead of a command value. A scheduled event, in Axon's sense, is documented as a scheduled command to the aggregate that emits it.

## Design decision 4: combined logic testing stays outside the framework

A combined logic is a stream handler that sends commands. Testing one needs events in, commands out, and (after S6) time. That is all test-side: `TestableCombinedLogic` runs the real combined logic on `inmemory-journal` (which has the query side Pekko's `inmem` journal lacks) with a recording gateway; no new abstraction is added to `bounded-core`.

## Implementation backlog

IDs are prefixed with S so they do not clash with the numbers in [event-sourcing-projection-fixes](event-sourcing-projection-fixes.md); plain numbers in "Depends on" refer to that plan. Size: S under a day, M a few days, L a week or more.

### Milestone A: test a combined logic (test helpers only)

| # | Priority | Depends on | Change | Test to add | Size |
| --- | --- | --- | --- | --- | --- |
| S1 | P1 | — | `TestableProjection.startProjection` accepts any `Resumable` (combined logic included), keeping the `AbstractEventMaterializer` overload so callers compile. `givenEvents` / `addEvents` store each event under its own `id` (group by `id`, keep order within an id) instead of all under `evt.head.id`. A combined logic reads several aggregate types, so the events can come with their type: `givenEvents(aggregateType -> events, ...)` next to the current one-type form, each stored under `aggregateType.persistenceId(id)` and tagged with the type. Waiting for processing uses `EventProcessed` as now, which combined logic already publish | Projection spec unchanged; a combined logic over two aggregate types runs in `TestableProjection`; events of two aggregates in one `givenEvents` call land in two persistence ids | S |
| S2 | P1 | — | `RecordingTypedCommandGateway[T]` (implementing `TypedCommandGateway`, so it replaces the local or the sharded gateway) and `RecordingCommandGateway` (classic) in `bounded-test`. Both run `ValidateableCommand` like the real gateways, record every command in order (`commands`, `commandsFor(id)`, `clear()`), and answer through a reply function the test sets (typed: `PartialFunction[T, Unit]` that answers the command's `replyTo`; classic: `DomainCommand => Any`, default `Ok(Nil)`). A command without a reply rule fails the ask with a clear message instead of timing out | Records in order, validation failures surface as `CommandValidationException`, reply rules apply, missing rule fails fast | S |
| S3 | P1 | S1, S2 | `TestableCombinedLogic`: `TestableCombinedLogic(combinedLogic, gateway).givenEvents(...)` / `givenNoPriorActivity` (also clears the in-memory journal and offset store), `whenEvent(s)(...)`, then `dispatchedCommands`, `expectDispatchedCommands(cmds*)`, `expectNoDispatchedCommands()`. Starts the combined logic after the given events, and records only commands sent after the `when` step, matching Axon's split between history and the event under test. Works for typed and classic gateways | A combined logic over two aggregate types that sends a command per event: given/when/then passes; a combined logic that sends nothing passes `expectNoDispatchedCommands`; replays from the given events do not count as dispatched | M |

### Milestone B: time and deadlines

| # | Priority | Depends on | Change | Test to add | Size |
| --- | --- | --- | --- | --- | --- |
| S4 | P1 | — | `BoundedTime` extension with `clock`, config `bounded.time.clock = system \| manual` and `bounded.time.manual-start`. `ManualClock` in `bounded-core` (needed at runtime by the extension); `TestTime` in `bounded-test` moving `ManualClock` and `ManualTime` together, with a fail-fast check that both are configured | `advance` moves `Instant` and fires a scheduled task that sees the new time; system without `ManualTime.config` gets the clear error | S |
| S5 | P1 | S4 | `DeadlineSet` / `DeadlineCleared` traits (no tag: the service reads the owners' aggregate types). `TestableAggregateRoot` (classic and typed) gains `pendingDeadlines`, computed from the given and emitted events, so aggregate tests can assert on deadlines without running the service | Aggregate test sets, moves (same id) and clears a deadline; `pendingDeadlines` follows | S |
| S6 | P1 | S4, S5, 5 (done) | `DeadlineService(aggregateTypes, deliver, ...)`: reads the owners' aggregate types through `AggregateTypeReadJournal` from the beginning on start into memory (offset per type, as `TagOffsets`), schedules deadlines inside the window with the Pekko scheduler, re-reads every interval, delivers overdue deadlines at start, retries delivery with backoff, drops a deadline on `DeadlineCleared` (cancelling its task), reads the time only from `BoundedTime`. Started like a materializer, node-local (one instance per system), with the same restart settings; implements `StoppableResumable` | Runs on `inmemory-journal` and on R2DBC (Postgres, Testcontainers). With `TestTime`: deadline fires exactly at `dueAt`, not before; cleared deadline never fires; moved deadline fires once at the new time; delivery failure retries, then succeeds; deadline due during downtime fires after restart; deadline beyond the window fires after the next re-read; an aggregate that handles the expiry and persists `DeadlineCleared` gets one delivery | L |
| S7 | P2 | S6 | `CommandScheduler` (design decision 3): an aggregate of type `bounded-command-scheduler` built with `AggregateBehavior`, `schedule` / `cancel`, delivery through `DeadlineService`, command factories by name for typed commands with `replyTo`. Document "scheduled event = scheduled command to the aggregate that emits it" | Schedule, replace, cancel; survives an actor system restart on a persistent journal; a combined logic schedules a follow-up command and it arrives after `TestTime.advance` | M |
| S8 | P2 | S6, 4 and 10 (done) | `DeadlineStore` with in-memory, JDBC (Slick), R2DBC and Cassandra implementations: pending deadlines plus the service's offset, written together; `getDue(until: Instant)` for the window read. Selected like offset stores (`bounded.deadlines.store.type`); S6's replay-into-memory stays the default for `inmemory-journal` | Per store (H2, Postgres and Cassandra through Testcontainers as for offset stores): store, move, clear, read window, offset written with the change; service restart reads only the window, not the history of the owners' types | M–L |
| S9 | P2 | S3, S4, S6 | Time in the test helpers. `TestableCombinedLogic` and `TestableAggregateRoot`: `givenCurrentTime(instant)`, `whenTimeElapses(d)`, `whenTimeAdvancesTo(instant)` (with `andThen` variants where the fluent style needs them), `expectScheduledDeadline(...)`, `expectTriggeredDeadlines(...)`. In `TestableCombinedLogic` this runs a real `DeadlineService` with a recording `deliver`, so deadlines set by aggregates and commands scheduled by the combined logic both show up | Combined logic schedules a reminder, time advances past it, reminder command is dispatched; aggregate sets a deadline, `whenTimeElapses` triggers it and the expiry command is handled | M |

### Running clustered

| # | Priority | Depends on | Change | Test to add | Size |
| --- | --- | --- | --- | --- | --- |
| S10 | P2 (clustered) | S6, S7, 15 and 17 and 22 (done) | Run the `DeadlineService` exactly once per cluster with `ClusteredEventMaterializers` (item 15 chose `ShardedDaemonProcess`; the service is a `StoppableResumable`, so it moves to another node with its offsets when its node leaves). The `CommandScheduler` is an aggregate, so in a cluster it is reached through `ShardedTypedCommandGateway` like any other; its commands and events need serializer bindings (`ReplySerializer` covers the replies). The `deliver` function uses a sharded gateway so expiry commands reach owners on any node. Until then document that the service must run on one node only: on two nodes every deadline is delivered twice (harmless when owners are idempotent, but wasteful) | Two systems in one JVM with `ClusterNodes` (as `ClusteredEventMaterializersSpec`): each deadline is delivered once; after the node running the service leaves, pending deadlines still fire on the other | M |

### Documentation

| # | Priority | Depends on | Change | Size |
| --- | --- | --- | --- | --- |
| S11 | P2 | S3, S6, S7 | User docs: testing a combined logic; deadlines owned by aggregates (set, move, clear, handle expiry idempotently, give the service the owners' aggregate types); when to use the `CommandScheduler`; at-least-once delivery; reading time through `BoundedTime`; the window and interval settings; one service per cluster | S |

## Order of work

1. **S1, S2, S3** first: they help today, change no framework code, and give the test harness the deadline work needs.
2. **S4** before anything that reads time, so the deadline service never calls `Instant.now()` directly.
3. **S5, then S6.** S6 builds on the restart and offset work (item 5 of the other plan, done) so a failing delivery or store write does not stop the service for good.
4. **S9** right after S6, so the deadline features are testable the way users will test them.
5. **S7, S8** when there is demand: S7 for combined logic that needs timeouts, S8 when start-up replay of the owners' aggregate types becomes slow.
6. **S10** once S6 and S7 exist; the clustered building blocks it needs (items 15, 17, 22) are done.

## Breaking changes

None of the items change existing APIs. S1 adds overloads and changes `givenEvents` to store events under their own ids; tests that relied on all events sharing the first event's id must pass them in separate calls. New config keys live under `bounded.time` and `bounded.deadlines` (there is no `bounded.deadlines.tag`).

## Not planned

| Axon element | Why not |
| --- | --- |
| Saga instances, association values, `expectActiveSagas` | A Bounded combined logic is one stream handler, not one instance per business transaction. Per-transaction state belongs in an aggregate (a process manager) that the combined logic sends commands to; that aggregate can own deadlines (S5) |
| `fieldFilters` | ScalaTest matchers and case-class `copy` cover ignoring fields |
| A stub deadline manager inside the aggregate test | `pendingDeadlines` (S5) checks what was set; S9 runs the real service on test time, which tests the actual delivery path |
| A Bounded test helper for `bounded-pekko-projection` | Pekko's `ProjectionTestKit` runs a projection from the store factories directly; `BoundedProjectionsSpec` shows how |
