# Read-side waiting, event metadata and a versioned JSON event serializer

Nov 7, 2026 · @Olger Warnier · extracted from the ev-operations migration plan (`ampsflow/ev-operations/plans/ddd-cqrs-migration/plan.md`, decision D3 of Nov 7, 2026, and the Bounded facts its D1, D2 and Steps 4–8 rely on) · validated against `main` at `bdda157` (tag `v2.0.1`)

## Summary

The ev-operations backend moves to DDD / CQRS on Bounded, in Bounded's **Scala 2.13 build**, inside the case engine's JVM and on the engine's JDBC journal. Its plan (D3) puts three generic parts in Bounded instead of in the backend, because any Bounded user with a read side, an audit need or stored events needs them:

- **W. Waiting for the read side** ("read your own write"): a reply that says where the command's events are, and a registry that completes once a chosen projection has processed them.
- **M. Event metadata**: command id, correlation id, causation id, actor, timestamp and application fields, from the command to the events it causes, and on to materializers and projections.
- **S. A versioned JSON event serializer**: a serializer base (fixed id, manifest `<EventName>.v<n>`, upcasters) and a `bounded-test` helper that checks stored JSON fixtures.

Bounded has none of the three today (checked). Validation against the code changes the plan in a few places (see [Validation](#validation-against-the-code)):

- **The version.** `v2.0.1` is tagged on `main` and the Scala 3 artifacts are on Central, so these parts cannot land in "2.0.1-SNAPSHOT". W changes the shape of a reply, so they go in **2.1.0** (`2.1.0-SNAPSHOT` until released). The `_2.13` artifacts of 2.0.1 are **not** on Central (checked Nov 7), which the backend needs for production anyway.
- **W needs two Bounded fixes first**: a projection publishes nothing for an event its filter rejects, so a waiter for such an event never completes; and `EventProcessed` names its projection only by a random UUID.
- **M cannot use journal metadata.** Pekko 1.7's typed persistence has no public API to store metadata next to an event, and the gateway cannot change user-defined commands or events. The metadata travels **in** commands and events, as an opt-in Bounded type, with helpers to pass it on.
- **The typed test helper cannot test the engine-first aggregates** the backend plans (replies and `Ko` refusals are not visible, and an asynchronous `pipeToSelf` step is not waited for). An item T is added.

Estimate: W 4–5 days (including failure reporting, W2a), M 2 days, S 2–3 days, T 1–2 days, release 1 day; all on both Scala versions.

## What the backend relies on (validated)

These are the Bounded facts in the ev-operations plan, checked against the code. ✔ holds, ✱ holds with a remark, ✘ is wrong or outdated.

| # | Claim in the ev-operations plan | Status | Evidence / remark |
|---|---|---|---|
| V1 | Bounded 2.0.0 on Central as `_3`, Pekko 1.7.1, pekko-persistence-jdbc 1.3.0, Slick 3.6.1, Projections 1.1.0 | ✔ | `project/Dependencies.scala`; `bounded-core_3` metadata on Central now says latest **2.0.1** |
| V2 | The 2.13 build is "2.0.1-SNAPSHOT on branch `scala-2.13-cross-build`, not yet on Central" | ✘ outdated | The cross build is merged (`bdda157`, #80) and tagged **`v2.0.1`**. `bounded-core_3:2.0.1` is on Central; `bounded-core_2.13:2.0.1` and `bounded-pekko-projection_2.13:2.0.1` return 404 and the `_2.13` metadata still says 1.0.0. `~/.m2` holds only `_2.13` `2.0.1-SNAPSHOT`. Release step 5 of `plans/scala-2.13-cross-build.md` is still open for 2.13 |
| V3 | Persistence ids are `type\|id`; the aggregate id is `AggregateType.aggregateIdOf(persistenceId)` | ✔ | `AggregateType.scala:37,58,67`; `TypedAggregateRootManager.behaviorFor` |
| V4 | On the JDBC journal every event is tagged with the aggregate type name; materializers read by that tag | ✔ | `AggregateBehavior.typeTag` (`withTagger`), `ReadJournalProvider` (jdbc → `JdbcReadJournal`, tag queries) |
| V5 | Bounded picks the engine's plugins | ✔ | Bounded follows `pekko.persistence.journal.plugin`; the backend sets `jdbc-journal` (`application.conf:77`), so Bounded reads through `jdbc-read-journal` with the engine's table names |
| V6 | Index for tag queries: `(tag, event_id)` on the tag table | ✔ | README, "Journals" (JDBC journal). On the backend the table is `case_event_tag` |
| V7 | "Bounded uses the same plugin instance and creates nothing" (D2, point 3) | ✱ | True for the journal. Not for combined logic on `ReadJournalOffsetStore`: on JDBC that is `JdbcOffsetStore`, which runs `createIfNotExists` for table `OFFSET` on first use (`JdbcOffsetStore.scala:48`), in the journal's Slick database unless `bounded.eventmaterializers.jdbc-offsetstore.database-config` is set. The backend's schema `OperationsDB_1_0_5` should create `OFFSET` itself (column `OFFSET` VARCHAR(4000)), or accept that Bounded creates it |
| V8 | Bounded's gateway messages (`SpawnAggregate`) have no serializer; Bounded's tests run with `serialize-messages = off` | ✔ | `DefaultTypedCommandGateway` guardian protocol; `SpecConfig.scala:27` (core and test). `Ok`/`Ko` do have a serializer (`ReplySerializer`, `reference.conf`). Optional Bounded fix, item X1 |
| V9 | `BoundedSlickProjection.exactlyOnce` writes and stores the offset in one transaction; `EventProcessed` after the commit when `bounded.eventmaterializers.publish = on`; a `withStatusObserver` in `adjust` replaces it | ✔ | `BoundedSlickProjection.scala:36–49`, `BoundedProjections.publishEventProcessed` (a `StatusObserver.afterProcess`) |
| V10 | `EventProcessed(materializerId, offset, persistenceId, sequenceNr, event)` | ✱ | `EventProcessed.scala:10`. `materializerId` is `UUID.randomUUID()` per `EventProjection` / materializer instance (`EventProjection.scala:36`), so a listener can only tell projections apart if it holds the instance. **Events the filter rejects are not published** (`projection.accepts(event)`, `EventStreamHandling.scala:92`). Both matter for W |
| V11 | `Ok(events)` has neither persistence id nor sequence number | ✔ | `Protocol.scala`. Typed aggregates reply with their own reply type through the ask's `ActorRef[Res]`; `Ok`/`Ko` are used by classic aggregates and by typed ones that choose them |
| V12 | The reply's sequence number is `EventSourcedBehavior.lastSequenceNumber` after `persist` | ✱ | Exists in Pekko 1.7.1, but takes an `ActorContext`; `AggregateBehavior` builds an `EventSourcedBehavior` without one, so the behavior has to be wrapped in `Behaviors.setup` (item W1) |
| V13 | Engine-first: wrap `AggregateBehavior.withEnforcedReplies` in `Behaviors.setup` for `pipeToSelf`, stash with `Effect.stash` while waiting | ✱ | Works: the gateways and the typed `TestableAggregateRoot` only spawn `behaviorFor(id)`, and the persistence-id check runs the wrapped behavior. Two remarks: the internal results (`EngineAccepted`, `EngineRefused`) must be of the command type `T <: DomainCommand`, so they carry `aggregateRootId` (and need a serializer once the aggregates are sharded); the gateway has **one** ask `Timeout` for all commands, which must exceed the engine timeout |
| V14 | Typed `TestableAggregateRoot` for given/when/then, also for engine-first commands against fake ports | ✘ partly | Given/when/events works on a wrapped behavior. But `failure` only reports exceptions thrown by the aggregate, not a `Ko` or other refusal reply; there is no access to the reply at all; and `when` waits for one event-stream message and then 500 ms, which does not cover an asynchronous engine step. Item T |
| V15 | `ShardedTypedCommandGateway` and `ClusteredEventMaterializers` for a cluster later | ✔ | `aggregate/typed/ShardedTypedCommandGateway.scala`, `eventmaterializers/ClusteredEventMaterializers.scala` |
| V16 | "A serializer must not throw on an event it stored before" (operating notes) | ✔ | README, "Operating notes". Bounded ships no serializer for user types; spray-json 1.3.6 is already a `bounded-core` dependency (`Dependencies.scala:37`) |
| V17 | Bounded 2.0 has no read-side waiting, no event metadata and no event serializer base | ✔ | Nothing on `main`. Metadata existed until 0.3.0 and was removed on purpose: "Removed all additional required elements for Commands and Events" (#62, `dda354b`). The test protocol still shows the pattern as user code (`TestCommandMetaData`, `TestMetaData.fromCommand`) |

## Items

### W. Waiting for the read side

The backend wants a write endpoint to answer only once the legacy table shows the write, as the case engine does with its last-modified registration (`LastModifiedRegistration.waitFor`, completed by the query-DB projection after its commit). The signal is the projection's commit; the key is the persistence id and the sequence number of the command's last event, not a correlation id (one request can send several commands with one correlation id, and a sequence number also covers "at or past").

| # | Change | Validation remark | Size |
|---|---|---|---|
| W1 | **Position in the reply.** A `EventPosition(persistenceId: String, sequenceNr: Long)`; a reply that carries it. Options: (a) `Ok(events, position: Option[EventPosition] = None)`, which breaks every `case Ok(events)` pattern and the `ReplySerializer` format (needs a manifest version); (b) a new `Reply` case `Persisted(events, position)` next to `Ok`. Prefer (b): existing code keeps working, `ReplySerializer` gets one more manifest. For typed aggregates, a helper that gives the command handler the `ActorContext` (`AggregateBehavior.withContext(persistenceId, …)((ctx, state, cmd) => …)`, a `Behaviors.setup` around the `EventSourcedBehavior`) and `AggregateBehavior.position(ctx)` = `EventSourcedBehavior.lastSequenceNumber(ctx)` for use in `thenReply`. `AggregateRootActor` (classic) replies `Persisted` with `lastSequenceNr`. The engine-first wrapping (V13) uses the same `setup` | V11, V12. Changing what `AggregateRootActor` replies is a behaviour change: make it opt-in (`replyWithPosition = true`) or accept it in 2.1.0 with a release note | M |
| W2 | **A progress signal for every event, filtered ones too.** Today a projection publishes `EventProcessed` only for accepted events, so a waiter for an event the projection filters out never completes. Add `ProjectionProgress(projectionId: ProjectionId, persistenceId, sequenceNr)`, published from the same status observer (`afterProcess`) for every envelope, accepted or not, when `bounded.eventmaterializers.publish` is on; `EventProcessed` stays as it is. The same for `AbstractEventMaterializer` (key: `viewIdentifier`), so W also works on node-local materializers | V10. Leaving `EventProcessed` unchanged keeps current listeners working; the stable name (`ProjectionId` / `viewIdentifier`) replaces the random `materializerId` as key | S |
| W2a | **A failure signal.** The same observer publishes `ProjectionFailure(projectionId, persistenceId, sequenceNr, cause, final: Boolean)` from `error(projectionId, envelope, cause, recoveryStrategy)` and `ProjectionDown(projectionId, cause)` from `failed(projectionId, cause)`. Pekko calls `error` on every failed attempt, retries included, with the envelope and the strategy (Projections 1.1.0, `HandlerRecoveryImpl.scala:95,112`), and `failed` then `stopped` when the stream fails (`InternalProjectionState.scala:488`). `final` is true for `fail` and `skip`, and for `retryAndFail(n, _)` / `retryAndSkip(n, _)` on attempt n + 1 (the observer counts attempts per envelope); earlier attempts are published with `final = false`. For a `groupedWithin` projection the envelope is the one Pekko passes, so the failure covers the group's events of that persistence id. `AbstractEventMaterializer` / `EventStreamHandling` publish the same two messages for a failing handler and for a stream that gives up after `bounded.eventmaterializers.restart.max-restarts` (key: `viewIdentifier`) | A projection works in order, so a failure on one entity also holds up the events of every other entity behind it; `ProjectionDown` is how those waiters learn it. A down projection may restart with backoff and catch up, so `ProjectionDown` means "down now", not "never processed" | S |
| W3 | **`ReadSideRegistry`** (bounded-core, an extension or a plain class on an `ActorSystem`): subscribes to `ProjectionProgress`, `ProjectionFailure` and `ProjectionDown`; per (projection, persistence id) the highest sequence number since startup and the waiting promises. `awaitProcessed(projection, position, timeout): Future[ReadSideResult]` ends in `Processed`, `Pending(reason)` or `Failed(cause)`; programming and infrastructure errors fail the future (see [Registry outcomes](#registry-outcomes)). Entries without waiters are dropped after `bounded.read-side.idle-retention` (default 5 min); default timeout `bounded.read-side.timeout` (5 s). A reply without a position (no events) does not wait. Node-local | Since the registry only knows what it saw since startup, a waiter for an event processed before a restart, or after its entry was dropped, waits for the timeout and gets `Pending(Timeout)`. That is why a reply without events must not wait, and why the idle retention must be far above the projection lag. Document both | M |
| W4 | **Cluster variant**: `ProjectionProgress`, `ProjectionFailure` and `ProjectionDown` over a Pekko distributed pub-sub topic (`pekko-cluster-typed` `Topic`) when the projections run under `ClusteredEventMaterializers` / `runInCluster`, so a waiter on node A completes when the projection runs on node B | Not needed by the backend (one node); follow-up | S |
| W5 | Tests (both Scala versions): the registry with an `inmemory-journal` Slick/JDBC projection on H2 (wait completes after commit, at-or-below completes at once, filtered event completes, timeout gives `Pending(Timeout)`, idle entries dropped; a handler that fails with `fail` gives `Failed` and with `skip` too, `retryAndFail(2, _)` gives `Failed` only after the third attempt and `Processed` when the second attempt succeeds, a failure on another entity gives `Pending(ProjectionDown)`, a projection that restarts after `ProjectionDown` completes later waiters with `Processed`, a waiter registered after a final failure gets `Failed` at once; an unknown projection id, a registry that is not started and a terminating actor system give failed futures; the same for a node-local materializer); `Persisted` through `ReplySerializer` in `ReplySerializerSpec`; position equals the stored sequence number for classic and typed | | M |

#### Registry outcomes

The outcomes of W3 are split by what the caller can do next:

```scala
sealed trait ReadSideResult
case object Processed extends ReadSideResult                           // the read model shows the write
final case class Pending(reason: PendingReason) extends ReadSideResult // may still show it: Timeout | ProjectionDown(cause)
final case class Failed(cause: Throwable) extends ReadSideResult       // will not show it without intervention
```

At or below the known number completes `Processed` at once, so registering after the reply has no race. `Pending(Timeout)` when the timeout passes; `Pending(ProjectionDown(cause))` at once for every waiter of a projection that published `ProjectionDown` (it may restart with backoff and catch up, so it is transient, like a timeout). `Failed` only on a final `ProjectionFailure`, for the waiters of that persistence id at or past the failed sequence number (with `skip` the offset moves on but the entity's read model misses the event, so it is final too); a non-final failure completes nothing. The registry keeps the last final failure per (projection, persistence id), so a waiter that registers after it also gets `Failed` at once; a later `ProjectionProgress` past that number clears it. **Neither `Pending` nor `Failed` means the command failed**: its events are stored, so the caller must not retry or compensate the command. `Pending` maps onto asynchronous request-reply (202 with `Location`); `Failed` is the outcome worth an alert.

These expected outcomes are values, not failed futures, as with Bounded's `Reply = Ok | Ko`: a failed future reads as "the operation failed" to every caller, and a façade's default `recover` to 500 would make clients repeat a command that succeeded. The same reasoning is behind RavenDB's `throwOnTimeout = false` option; Marten and Pekko's ask fail on a timeout instead, and none of them reports projection failures to the waiter. **Programming and infrastructure errors are failed futures**: a projection id the registry does not know (no `EventProjection` / materializer with that id registered with it, so the wait could never complete; fail with `IllegalArgumentException` instead of turning every write into a timeout), a registry that is not started, and an actor system that is terminating (`IllegalStateException`). No exception-throwing variant (`awaitProcessedOrFail`): callers map the result themselves.

Stays in the application: which projection a request waits for, the timeout setting, and what `Pending` and `Failed` mean to its clients (the backend answers 202 with a `Location` header on `Pending`; on `Failed` it decides between the same 202 and a status of its own, and logs, counts or alerts with the correlation id).

### M. Event metadata

| # | Change | Validation remark | Size |
|---|---|---|---|
| M1 | **Types**: `CommandMetaData(commandId: UUID, correlationId: String, causationId: Option[String], actor: String, timestamp: Instant, fields: Map[String, String])` and `EventMetaData` (the same plus `migrated: Boolean`), with `EventMetaData.causedBy(command: CommandMetaData)` (causation = the command id, the rest copied) and `CommandMetaData.causedBy(event: EventMetaData)` for policies. Traits `HasCommandMetaData { def metaData: CommandMetaData }` and `HasEventMetaData`, **opt-in**: `DomainCommand` and `DomainEvent` do not change | V17: #62 removed required metadata on purpose; opt-in keeps that decision. The timestamp comes from an injected `java.time.Clock` (see `plans/combined-logic-testing-deadlines-and-time.md`) | S |
| M2 | **Where it is stored: in the event.** Pekko 1.7 typed persistence has no public API for per-event journal metadata (only the internal `ReplicatedEventMetadata`), and a typed `EventAdapter` sees the event, not the command. So the event carries its `EventMetaData` as a field, the aggregate fills it with `causedBy(cmd.metaData)`, and S serializes it next to the payload (an envelope `{ "meta": …, "data": … }`, so the event codec does not have to know it) | The D3 wording "passed by the gateway from the command to the events" is not possible: the gateway sees user-defined commands and never sees events. The application builds the `CommandMetaData` (the backend: actor from the JWT `sub` or `system:engine`, correlation id from the request id, tenant as a field) and the aggregate copies it | S |
| M3 | **Access on the read side**: materializers, combined logic and projections get the event, so `HasEventMetaData` gives them the metadata; a helper `EventMetaData.of(event): Option[EventMetaData]`. `CombinedLogicEventMaterializer` users derive a command's metadata with `CommandMetaData.causedBy` | | S |
| M4 | **Gateway**: when a command `HasCommandMetaData`, the gateways put correlation id and command id in the logging MDC while they validate and deliver; a `ValidateableCommand` can refuse a command without metadata. Nothing else changes | Optional; cheap, helps the "failed commands logged with correlation id" requirement | S |
| M5 | Tests: `causedBy` chains; a typed and a classic sample aggregate that copy the metadata; metadata through S's serializer and back | | S |

### S. Versioned JSON event serializer

| # | Change | Validation remark | Size |
|---|---|---|---|
| S1 | **`VersionedJsonSerializer`** (bounded-core), a `SerializerWithStringManifest` base: the subclass gives the fixed `identifier`, and per event class a name, a current version and a codec; manifest `<EventName>.v<n>`; payload UTF-8 JSON; per older version an upcaster (JSON → JSON of the next version), applied in order on read; an unknown manifest or a failing codec throws `NotSerializableException` with the manifest (Pekko's contract) — the "never fails on a stored event" rule is enforced by S2's fixtures, not by swallowing errors | Codec-agnostic: the codec type is `T => String` / `String => T` plus a JSON-tree upcaster on a small Bounded `Json` abstraction, or spray-json `JsValue` since spray-json is already a `bounded-core` dependency (V16). Prefer spray-json for the upcasters and plain functions for the codecs, so an application on circe or Jackson can still use the base. The identifier must not clash with Pekko's reserved ids (0–40), with `ReplySerializer`'s or with others in the system (the backend: the engine's `CafienneSerializer`); the base checks at startup that no other serializer in `SerializationExtension` has its id | M |
| S2 | **Fixture check in `bounded-test`**: `EventFixtures.verify(serializer, folder)` reads `<folder>/<EventName>.v<n>.json` (one stored sample per event version), deserializes each with the current serializer and, for the current version, checks that serializing again gives the same JSON; reports every failing file; and `EventFixtures.missing(serializer, folder)` lists event versions without a fixture. Framework-neutral results, with a ScalaTest wrapper | `bounded-test` is a main artifact used from test scope; keep ScalaTest use inside the wrapper | S |
| S3 | **Metadata envelope**: when an event `HasEventMetaData`, S1 writes `{ "meta": …, "data": … }` and reads both back, so M2 needs no codec support | Depends on M1 | S |
| S4 | Tests: round trip on both Scala versions; an upcast chain v1 → v3; unknown manifest; fixtures folder with a broken sample; a persisted event replayed through the JDBC journal on H2 with the serializer bound | | S |

Stays in the application: one serializer per context with its own id, its bindings, the event codecs and the fixtures.

### T. Testing engine-first typed aggregates

| # | Change | Validation remark | Size |
|---|---|---|---|
| T1 | Typed `TestableAggregateRoot.whenAsk(replyTo => command)`: sends the command with a probe as reply ref, waits for the reply (the caller's timeout) and keeps it: `reply`, and `failure` also for a `Ko` reply. Then `events` covers what the command persisted, also after an asynchronous step (`pipeToSelf`) in the behavior, because the reply comes after the persist | V14. `when` stays as it is | S |
| T2 | `thenWhenAsk` for a second command while the first still waits (stash), and an assertion on the reply order | | S |

### X. Small fixes found during validation

| # | Change | Size |
|---|---|---|
| X1 | Mark the gateway guardians' control messages (`SpawnAggregate`, `StopAggregate`, …) `NoSerializationVerificationNeeded`, so an application can run with `serialize-messages = on` as long as its own commands and replies have bindings. Not needed by the backend (it sets `serialize-messages = off`) | S |
| X2 | README "Journals": `JdbcOffsetStore` creates table `OFFSET` on first use in the journal's database; say so, with its DDL, for applications whose schema is managed elsewhere (V7) | S |

## Order and release

1. W2, W2a, X1 (no API break), then W1, W3, W5.
2. M1–M3, M5, then S1–S4 (S3 needs M1).
3. T1, T2 (the backend's Phase 1 needs them for its engine-first tests).
4. W4 and M4 when wanted.
5. Release **2.1.0** for Scala 3 and 2.13 (`+publishSigned; sonaRelease`), and check that the `_2.13` artifacts reach Central this time (V2). Until then the backend builds against `2.1.0-SNAPSHOT` from `+publishM2`.

All items build and test on both Scala versions (`sbt "+test"`), in shared sources without Scala 3-only constructs.

## Corrections for the ev-operations plan

- Replace "2.0.1-SNAPSHOT on branch `scala-2.13-cross-build`" with: merged to `main`, tagged `v2.0.1`, `_3` on Central, `_2.13` not yet on Central; D3 lands in **2.1.0**.
- D3 "passed by the gateway from the command to the events" → the façade builds the command metadata, the aggregate copies it into its events (M2).
- Step 4, point 8: the signal is the new `ProjectionProgress` (also for filtered events), not `EventProcessed`. The wait ends in `Processed`, `Pending` (timeout, or the projection is down) or `Failed` (the projection will not show the event without intervention); decide what the façade answers on `Failed`, knowing the command itself succeeded. A failed future means a configuration or infrastructure error (e.g. an unknown projection id), not a slow read side.
- D2, point 3: `ReadJournalOffsetStore` on JDBC creates table `OFFSET`; add it to `OperationsDB_1_0_5` or accept the creation.
- Step 8: engine-first tests use `whenAsk` (T1); the gateway's single ask timeout must exceed the engine timeout.
