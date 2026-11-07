# Scala 2.13 release next to Scala 3

Nov 6, 2026 · @Olger Warnier · based on a trial 2.13 build of `main` at `d031e13`

## Summary

**Status (Nov 6, 2026): steps 1–4 are done** on branch `scala-2.13-cross-build`. S1–S6 and the two test warnings are fixed, both versions compile with no warnings (`+Test/compile`), and `+testFull` passes all 194 tests on Scala 3.3.9 and on 2.13.18. CI runs the 2.13 tests, the README and CLAUDE.md are updated, and `+makePom` produces `_3` and `_2.13` poms with the right Scala library, cross-versioned `bounded-core` and the `Optional` projection modules. Still open: step 5 (the first `+publishSigned; sonaRelease` and the check with a 2.13 sample project).

Publish every Bounded module for Scala 2.13 (`bounded-core_2.13`, …) next to the Scala 3 artifacts (`bounded-core_3`), from **one shared source tree**, with the same version and in the same release. Applications that cannot move to Scala 3 yet (casefabric users on 2.13, older Pekko services) can then use the aggregate types, clustering and projection work of 2.x.

A trial build shows this is cheap:

- **All dependencies exist for 2.13** at the versions in `project/Dependencies.scala` (Pekko 1.7, Pekko HTTP 1.4, projections 1.1 / r2dbc 1.2, persistence jdbc/r2dbc/cassandra, `pekko-persistence-inmemory` 1.3.0, spray-json, scala-logging, Slick 3.6).
- **The code is nearly 2.13-compatible already**: it uses braces, `implicit` parameters and no enums, extension methods, opaque types, `given` definitions or macros. Scala 3 syntax that 2.13 accepts with `-Xsource:3` (`?` wildcards, `xs*` splices, `case _: A | _: B`) needs no change.
- **Six small source changes** (below) made all four modules compile on Scala 2.13.18, and **all tests pass on 2.13** (core 161, projection 23, plus the gateway/test-module specs; the Testcontainers specs included). The same patched sources compile on Scala 3.3.9 with `-Wunused:all` **without a single warning**, so no per-version source directories are needed.

Estimate: one to two days, most of it build, CI and release verification.

## Source changes

Found by compiling a copy of the repository with `++2.13.18!` until it was clean. None changes the public API.

| # | File | Problem on 2.13 | Change (valid on both) |
| --- | --- | --- | --- |
| S1 | `eventmaterializers/offsetstores/CreateOnFirstUse.scala`, `bounded-test` `TestableProjection.scala`, `pekko/http/test/ScalatestTypedActorHttpRoute.scala` | `scala.compiletime.uninitialized` does not exist | `var x: T = _` (allowed in 3.3 without a warning; only `-source:future` deprecates it) |
| S2 | `BoundedR2dbcProjection.scala` (10×), `BoundedSlickProjection.scala` (5×), `bounded-test` `typed/TestableAggregateRoot.scala` (1×) | `(using x)` at a call site is Scala 3 syntax | pass the implicit argument as a plain argument list `(x)`; every callee declares `implicit` parameters, so Scala 3.3 accepts this silently |
| S3 | `BoundedR2dbcProjection.withSource` | the private `build` parameter is a polymorphic function type `[Envelope] => (…) => Result`, which 2.13 cannot express | a private `trait Build[Result] { def apply[Envelope](id, source, envelope): Result }`; the five factory methods pass `new Build[…Projection[Offset, ?]] { def apply[Envelope](…) = … }` |
| S4 | `aggregate/typed/ShardedTypedCommandGateway.scala:101` | 2.13 infers `systemActorOf[U]` and rejects `Behavior[Nothing]` | `system.systemActorOf[Nothing](…)` |
| S5 | `bounded-test` `LoggingTestProbe.scala:14` | `-Xsource:3` makes the inferred override type an error | explicit `: TestActor.AutoPilot` on `run` |
| S6 | `aggregate/typed/DefaultTypedCommandGateway.scala:56,58` | warning: outer reference in type test cannot be checked (case classes nested in an object inside the generic gateway class) | move the guardian protocol to the companion, or `@unchecked` on the patterns; one guardian per gateway, so the check is not needed |

Two 2.13-only test warnings (`ClassicCommandGatewaySpec.scala:87` compares `Unit == Unit`, `AbstractReplayableEventMaterializerWithRuntimeEventFilterSpec.scala:146` discards an `Assertion`) are real test smells and should be fixed rather than suppressed.

## Build changes

`build.sbt`, `basicSettings`:

```scala
val scala3   = "3.3.9"
val scala213 = "2.13.18"

scalaVersion       := scala3,           // stays the default: IDE, CI coverage, Sonar paths
crossScalaVersions := Seq(scala3, scala213),
scalacOptions := Seq("-encoding", "UTF-8", "-deprecation", "-feature", "-unchecked", "-Wvalue-discard") ++ {
  CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((2, _)) => Seq("-Xsource:3", "-Wunused:imports,privates,locals,implicits,nowarn", "-Wconf:cat=scala3-migration:e")
    case _            => Seq("-Wunused:all")
  }
},
```

- `-Xsource:3` is required: it enables `?` and `*` (48 files use `?` wildcards). Scala 3-only syntax that slips into a shared file then fails the 2.13 compile, which CI catches.
- 2.13 has no `-Wunused:all`; the explicit list matches what Scala 3 checks.
- The root project keeps `crossScalaVersions := Nil`, so `+` commands only cross-build the four modules.
- `scalafmt`: keep `runner.dialect = scala3` (shared sources are valid Scala 3); do not turn on the `rewrite.scala3` rules (`convertToNewSyntax`, `removeOptionalBraces`), which would produce syntax 2.13 rejects. Add a comment in `project/.scalafmt.conf` saying so.
- `sbt-buildinfo`, `sbt-header`, `sbt-git`, `sbt-scoverage` and `sbt-pgp` all work per Scala version; `BuildInfo` is generated for each.
- Scaladoc packaging (`Compile/packageDoc`, part of `publishSigned`) works on 2.13.

When a future feature cannot be written for both (e.g. `enum`, `opaque type`, `inline`), put the two variants in `src/main/scala-3/` and `src/main/scala-2.13/`, which sbt picks up per version automatically. Prefer keeping that out of the public API.

## CI

`.circleci/config.yml` currently runs `sbt "coverage; test; coverageReport"` on Scala 3 only.

- Keep that step as is: coverage and the Sonar report paths (`target/out/jvm/scala-3.3.9/...` in `sonar-project.properties`) stay on Scala 3.
- Add a step `sbt "++2.13.18; test"` (or a second job in parallel). It needs the same Docker setup for the Testcontainers specs.
- Cache: 2.13 adds a second set of dependencies to `~/.cache/coursier`; bump the cache key prefix (`v2-` → `v3-`) once.

## Releasing

Version handling (sbt-git, `GitInfoAnalyzer`) is unchanged; both Scala versions get the same `X.Y.Z`.

```bash
git tag vX.Y.Z && git push origin vX.Y.Z
sbt "+publishSigned; sonaRelease"     # stages _3 and _2.13 into localStaging, uploads one bundle
```

Snapshots: `sbt "+publishSigned"` publishes both to the Central snapshots repository.

To verify before the first real release (not tried in the trial, because it writes to `~/.ivy2/local` / Central):

1. `sbt "+publishLocal"` and check that both `bounded-core_3` and `bounded-core_2.13` poms carry the right Scala library and the `Optional` projection dependencies.
2. `sbt "+publishSigned"` from a snapshot and check that `localStaging` contains both cross versions before running `sonaRelease`, so they are in one deployment.
3. Resolve the 2.13 snapshot from a small sample project on Scala 2.13 and run a typed aggregate plus a materializer.

## Documentation

- `README.md` ("Bounded is built for Scala 3…"): list both Scala versions; the `%%` dependency lines already work for both.
- `CLAUDE.md`: the Project line (Scala 3 only), the Commands section (`+test`, `++2.13.18; test`, `+publishSigned`), a note that shared sources must compile on both (no `(using …)`, no `scala.compiletime`, no polymorphic function types, no `enum` / `extension` / `given` definitions outside `scala-3/` directories) and the release command.

## Order of work

1. Source changes S1–S6 and the two test warnings, verified on Scala 3 first (no behavior change, can land on its own).
2. `build.sbt` cross settings and per-version `scalacOptions`; `sbt "+Test/compile"` warning-clean on both.
3. CI step for 2.13 tests.
4. `+publishLocal` check, README and CLAUDE.md.
5. Next release: `+publishSigned; sonaRelease`, then the 2.13 sample-project check against Central.

## Open decisions

- **Support window.** Scala 2.13 is in maintenance; Pekko 2.0 (milestones on Central) still publishes for 2.13. Proposal: publish 2.13 for the 2.x line, and drop it at the first Bounded release that needs a Scala 3-only feature in the public API, announced one minor release ahead.
- **Binary compatibility checks.** Not part of this plan; with two artifact lines MiMa (`sbt-mima-plugin`) becomes more valuable, but it is equally useful for Scala 3 alone.
