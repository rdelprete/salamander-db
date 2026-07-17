# Changelog

All notable changes to `salamander-db` are documented here. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] — 2026-07-16

Physical log compaction ships. This is a minor (pre-1.0 breaking) release:
the manifest on-disk format gains retention fields, and once an application
runs a compaction, reads below the retention floor return the typed
`position_unavailable` result instead of the record. Databases that never
compact are unaffected; the floor defaults to zero and old manifests open
unchanged.

### Added

- **Retention observability** — Rust and Python now expose read-only status for
  generation/floor/head identity, proposed boundaries, blockers, consumer
  bootstrap readiness, maintenance handles, reclaimable bytes, and pending
  cleanup state.
- **Retention policy selectors** — exact positions, latest-event counts,
  timestamp cutoffs, and retained-byte targets now resolve deterministically
  into the existing blocker-aware explicit-floor plan in Rust and Python.

- **Normative retention and compaction contract** — defines the intentional
  durable-truth boundary after retention, explicit global floors, verified
  retention anchors, protected projection and branch coverage, consumer
  bootstrap, idempotency horizons, read-only planning, atomic generation
  replacement, typed unavailable results, and the failure tests that must pass
  before physical deletion can ship. The engine still retains all log bytes.
- **Non-destructive retention groundwork** — manifests now carry a
  backward-compatible global floor; Rust, facade, and Python expose it;
  historical reads below it return the stable `position_unavailable` result
  without clamping; and `plan_retention(keep_from)` reports the effective
  whole-segment floor, reclaimable bytes, and anchor/branch/projection/
  consumer/open-handle blockers. Planning never changes the floor or deletes
  files.
- **Verified engine-core retention anchors** — Rust, the engine facade, and
  Python can rebuild the stream and branch catalogs from verified log truth
  and publish a versioned, checksummed anchor for an effective floor and exact
  durable head. Reopen validates anchor identity before using it; a corrupt
  non-authoritative anchor falls back to the complete log. Anchor creation
  still does not advance the floor or delete segments.
- **Authoritative projection checkpoint coverage** — creating a retention
  anchor through the engine facade now checkpoints every registered
  projection and promotes the exact immutable snapshot IDs into anchor format
  v2. Planning revalidates each referenced snapshot's checksum, descriptor,
  branch, cursor, partition count, and unique partition identity; missing or
  corrupt coverage restores the projection bootstrap blocker.
- **Branch and consumer retention bootstraps** — applications can register
  opaque, checksummed checkpoints for a branch or durable consumer at the
  planner's effective floor. Anchor format v3 carries the bytes without
  interpreting them, rejects duplicate/mismatched/corrupt coverage, and clears
  branch or lagging-consumer blockers only after compatible coverage is
  promoted. Rust, the engine facade, and Python expose registration.
- **Atomic retention apply and closed-segment reclamation** — retention plans
  now carry opaque IDs and observed generations. `apply_retention(plan_id)`
  rejects unknown, blocked, or stale plans; atomically commits the new floor
  and authoritative anchor checksum in the manifest; then removes whole closed
  segments below the floor best-effort. A failed cleanup is only a space leak,
  while a corrupt committed anchor is rejected even if old-generation files
  remain.
- **Deterministic retention crash matrix** — the real-process harness now
  aborts at seven explicit boundaries spanning anchor creation, manifest
  publication, partial deletion, and completed cleanup. Every recovery must be
  wholly old or wholly new, then passes replay, branch, projection, feed,
  expected-revision, and pre-floor idempotent-retry oracles. The nightly Linux
  and Windows rotation includes this matrix.
- **Complete feed bootstrap recovery** — a lagging durable feed now receives a
  generation-bound descriptor on `position_unavailable`, including exact
  branch/stream/type scope, checkpoint identity, codec/version, length,
  checksum, floor, and resume continuation. Checkpoint fetch is separately
  bounded; resume rejects stale or altered descriptors and is tested for zero
  gaps and duplicates after physical compaction. Python exceptions expose the
  descriptor as `exc.bootstrap` and provide fetch/resume helpers.
- **Cross-platform and MSRV CI** — Rust and Python tests now run on Linux,
  macOS, and Windows for every pull request, with fail-fast disabled so one
  platform failure does not hide the others. A separate job runs the workspace
  suite on Rust 1.90, matching the crate's declared minimum toolchain. The
  nightly real-process crash harness now runs on both Linux and Windows through
  one platform-neutral PowerShell loop.

## [0.1.3] — 2026-07-15

### Added

- **First-class diff** — the divergence of two timelines as an engine
  operation ([docs/specs/first-class-diff.md](docs/specs/first-class-diff.md)):
  `Salamander::diff` returns the common ancestor, the exact divergence
  position, and three replay plans (shared prefix, each divergent suffix),
  computed from the branch catalog alone — no record is read or compared,
  and payload bytes are never consulted. Exposed through the facade
  (`Engine::diff`, emitting ready-to-open `ReplayRequest`s) and the Python
  binding (`db.diff`, returning a summary dict plus three pre-scoped
  readers). The DIFF contract is defended by catalog unit tests, an
  integration property test whose oracle is the brute-force
  double-replay-and-zip, and facade/Python suites; `chat.py`'s `/diff` now
  runs on the engine call, and `salamander/examples/08_diff.rs` shows the
  Rust surface.
- **`db.watch` in the Python binding** — the committed-batch feed as a
  blocking iterator: `tail -f` for the log. Yields events (the same row
  dicts as `replay`) only once durable, releases the GIL while waiting on
  the feed's commit signal, and stays Ctrl+C-responsive via chunked waits.
  `start=None` tails live from the durable head, `start=0` replays durable
  history then follows, `branch=` scopes to one timeline, `namespace=`
  filters per event, `timeout=` (seconds) ends the iteration when idle,
  and `consumer_id=` + `watch.ack()` persist a resumable checkpoint.
  Binding-only — the engine's feed (WP-08) already provided the
  subscription primitive. Nine-test pytest suite in
  `examples/py/test_watch.py`, including a cross-thread blocking wake.
- **Replay rows now carry `branch_id` and `namespace`** (additive) — so
  feed/watch consumers spanning branches and streams can tell rows apart
  without decoding metadata.
- **Typed, deterministic Python lifecycle** — database handles now support
  `with salamander.open(...) as db`, matching watch handles and releasing the
  single-writer lock deterministically. Wheels include a `py.typed` marker and
  `salamander.pyi` API definitions for editor and static-checker discovery.

### Changed

- **Clearer adoption boundary and documentation path** — the README now leads
  with durable execution history, distinguishes it from semantic memory, and
  includes explicit fit/non-fit guidance plus the current forever-retention
  limitation. New Python usage and upgrade guides document handle ownership,
  synchronous use from async applications, backups, schema evolution, and
  pre-1.0 compatibility expectations.

### Fixed

- **Inherited replay leaked grandparent records for forks created below
  their parent's own fork position.** `replay_scopes` capped each ancestor
  level only by the immediate child's fork position, so such a fork
  (legal, if odd) saw grandparent records in the window between the two
  fork points — contradicting `fork_branch`'s documented "inherits parent
  history up to `at`". The visibility caps now cascade as a running
  minimum from leaf to root. Found by the first-class-diff property test's
  double-replay oracle; pinned by catalog unit tests.

## [0.1.2] — 2026-07-14

### Fixed

- **Paged replay livelock** — `next_page` never reported `done` when the
  records past the last yielded one were all filtered out by the reader's
  branch scope (e.g. replaying the default timeline while a fork's events
  sit at the tail of the log). The page continuation now adopts the
  reader's continuation on an exhausted scan; a facade regression test
  drains both directions (default-over-branch-tail and
  branch-over-default-tail).

### Added

- **`fork(namespace, at, parent=…)` in the Python binding** — an optional
  `parent` branch name selects the timeline to fork from; it defaults to the
  root timeline, so this is backward compatible. The engine always supported
  forking any branch (`fork_branch`), so this only exposes an existing
  capability: forks of forks now work from Python, with `branch_ancestry`
  returning the full multi-level chain.
- **Python showcase demos** — `examples/py/chat.py` (a chat CLI where
  `/rewind`, `/fork`, `/branches`, `/switch`, and `/diff` are storage
  primitives; talks to the Claude API when available, deterministic mock
  otherwise) and `examples/py/unkillable_agent.py` (a self-supervising
  agent that is hard-killed mid-task and resumes from replay with
  exactly-once steps), each with an offline pytest suite. See
  [docs/specs/python-showcase-demos.md](docs/specs/python-showcase-demos.md).
- **The Undying Dungeon** — `examples/py/dungeon.py`, a browser roguelike
  served from the standard library alone: each turn is one atomic batch with
  recorded rolls (replay is deterministic), the timeline scrubber is
  `view_at`, dying forks a new future over the shared past, and a
  pull-the-plug button (`os._exit` mid-write) demonstrates crash-proof
  recovery. The bestiary panel is a registered view — a secondary index the
  engine maintains, queried without replaying the log. Twenty offline tests;
  see [docs/specs/dungeon-demo.md](docs/specs/dungeon-demo.md).

## [0.1.1] — 2026-07-13

Release automation only — no engine changes.

### Added

- **crates.io release workflow** — publishes the crate on version tags via
  Trusted Publishing, alongside the PyPI wheel workflow shipped in 0.1.0.
- **Python suite in CI** — the CI workflow builds the extension and runs
  the Python tests in a virtual environment.

## [0.1.0] — 2026-07-13

First public release — `salamander-db` on crates.io and abi3 Python wheels
on PyPI (Linux/macOS/Windows, built by maturin CI). The engine ships with
the complete post-v0.1 architecture, built on a set of normative
correctness contracts, from the stable on-disk format through instant
recovery.

### Storage and format

- **Format v2** — engine-owned record envelope (event/database/branch/stream
  identity, revisions, timestamps, typed metadata) over opaque payload bytes;
  self-delimiting CRC32C-checksummed frames; golden byte fixtures; explicit
  size limits enforced before allocation. Payload interpretation lives in
  codecs (`JsonCodec`, `BincodeCodec`), never in the engine (INV-9).
- **Segmented append-only log** — 64 MiB segment roll, atomic manifest,
  torn-tail truncation on open (a record either fully exists or never
  happened), interrupted-roll recovery, single-writer `LOCK`.
- **Offline v1 migration** — `salamander migrate` imports pre-release
  directories resumably and verifiably, with deterministic imported event IDs.

### Streams, batches, branches

- **First-class streams** — named streams with contiguous zero-based
  revisions, optimistic concurrency (`Any` / `NoStream` / `Exact`), and a
  rebuildable stream catalog.
- **Atomic batches** — multi-event appends are visible all-or-nothing across
  crash and recovery (INV-4); idempotency keys make retries safe: an identical
  retry returns the original receipt, a conflicting reuse appends nothing.
- **Durability levels** — every append declares `Buffered` / `Flush` / `Sync`;
  receipts state what was guaranteed instead of leaving it implied, with a
  documented per-platform survival matrix.
- **First-class branches** — durable, engine-owned ancestry with fork at any
  batch boundary, isolated inherited replay (children see parent history only
  through the fork point), archival, common-ancestor discovery, and one-time
  conversion of both legacy fork-marker protocols.

### Reading and subscribing

- **Bounded-memory streaming reader** — declarative `ReplayPlan` (branch,
  stream selector, position window, time filter, max events) executed with
  peak memory bounded by one read chunk plus the largest single frame, never
  by result count. Per-segment sidecar indexes (seek points, stream postings,
  timestamp ranges) let reads skip irrelevant segments without payload I/O;
  index loss or corruption changes I/O counts only, never answers.
- **Committed-batch feed** — a bounded, resumable feed of durable batches in
  global order (never buffered-only or partial batches), with branch/stream/
  type filters, consumer checkpoints, and explicit backpressure — the seam for
  subscriptions and future replication.

### Projections, snapshots, instant recovery

- **Projection runtime** — durable versioned descriptors; registered
  projections driven through one object-safe runtime; a failing projection is
  isolated at its last good cursor and can never fail an append or another
  projection; queries choose `RequireHead` / `AllowStale` / `WaitFor`
  consistency.
- **Verified snapshots** — atomic, checksummed projection checkpoints that
  identify their database, descriptor, branch lineage, and cursor. Every
  snapshot is treated as hostile cache input: anything missing, stale,
  corrupt, or mismatched is discarded and the engine falls back to an older
  snapshot or replay from the log. Deleting all derived state changes
  performance only, never answers.
- **Instant recovery** — open loads catalogs only: no snapshot load, no event
  replay, open time independent of log and projection size. Projection state
  is partitioned by a versioned `PartitionScheme` (stable `StreamId` hash) and
  heals lazily, Graefe-style: the first read touching a cold partition
  restores its newest valid snapshot and replays only that partition's suffix
  via a selective `ReplayPlan`. Failures mark one partition, never the
  database.

### API surfaces

- **Typed Rust engine** — payload-generic `Salamander<B>` over any serde
  payload; `AgentDb` (typed agent events, sessions, forks) and `JsonDb`
  (dynamic JSON) as provided vocabularies; live indexed query views
  (`get`/`range`/`prefix`/`by`) with synchronous fan-out; time-travel
  (`view_at`) and replay; group commit via `CommitPolicy`.
- **Engine facade** — a non-generic, thread-safe, language-neutral `Engine`
  with opaque handles, stable DTOs, typed error categories, and paged
  cursor-based replay: the single boundary all bindings share.
- **Python bindings** — `salamander-py` (PyO3/maturin) over the facade:
  `salamander.open`, dict-in/dict-out events, streams, branches, replay, and a
  LangGraph checkpointer that survives process restarts. Atomic batch append
  exposes optimistic concurrency, idempotent retries, explicit durability,
  event identity/type/schema/metadata, complete receipts, and stable typed
  exception categories without duplicating engine semantics in Python.
- **Playground** — `cargo run -p salamander-demo -- ui` serves a local
  zero-dependency web UI: append JSON events, scrub the time-travel slider,
  fork the timeline, and watch branches diverge.

### Verification

- Workspace test suite spanning unit, property-based (proptest), golden
  fixture, and integration tests; a `kill -9` crash harness over append,
  batch, snapshot publication, and heal paths, with a readiness handshake
  that targets each operation and deterministic scenario rotation; criterion
  benchmarks for open time, replay selectivity, snapshot restore, and instant
  recovery (`cargo bench -p salamander-db`).

### Not in this release (by design)

Multi-writer / multi-process access, a query language, vector search,
retention/compaction, network replication transport, and the swizzled
projection store — see [ROADMAP.md](ROADMAP.md) for what is planned versus
permanently out of scope.

[Unreleased]: https://github.com/rdelprete/salamander-db/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/rdelprete/salamander-db/releases/tag/v0.2.0
[0.1.3]: https://github.com/rdelprete/salamander-db/releases/tag/v0.1.3
[0.1.2]: https://github.com/rdelprete/salamander-db/releases/tag/v0.1.2
[0.1.1]: https://github.com/rdelprete/salamander-db/releases/tag/v0.1.1
[0.1.0]: https://github.com/rdelprete/salamander-db/releases/tag/v0.1.0
