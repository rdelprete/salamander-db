# Changelog

All notable changes to `salamander-db` are documented here. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] — unreleased

First public release. The engine ships with the complete post-v0.1
architecture, built on a set of normative correctness contracts, from the
stable on-disk format through instant recovery.

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
  LangGraph checkpointer that survives process restarts.
- **Playground** — `cargo run -p salamander-demo -- ui` serves a local
  zero-dependency web UI: append JSON events, scrub the time-travel slider,
  fork the timeline, and watch branches diverge.

### Verification

- Workspace test suite spanning unit, property-based (proptest), golden
  fixture, and integration tests; a `kill -9` crash harness over append,
  batch, snapshot publication, and heal paths; criterion benchmarks for open
  time, replay selectivity, snapshot restore, and instant recovery
  (`cargo bench -p salamander-db`).

### Not in this release (by design)

Multi-writer / multi-process access, a query language, vector search,
retention/compaction, network replication transport, and the swizzled
projection store — see [ROADMAP.md](ROADMAP.md) for what is planned versus
permanently out of scope.

[0.1.0]: https://github.com/rdelprete/salamander-db/releases/tag/v0.1.0
