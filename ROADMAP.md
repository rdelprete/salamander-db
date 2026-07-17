# SalamanderDB Roadmap

**Where things stand (July 2026):** the post-v0.1 architecture is fully
implemented — from the stable v2 format through streams, branches, the
streaming reader, the engine facade, the projection runtime, verified
snapshots, the commit feed, and instant recovery — and the first public
releases are out: `salamander-db` on
[crates.io](https://crates.io/crates/salamander-db) and Python wheels on
[PyPI](https://pypi.org/project/salamander-db/), tagged `v0.1.0` through
`v0.2.0`. See [CHANGELOG.md](CHANGELOG.md) for the full feature inventory.

This file is direction, not commitment: unreleased items may change or be
dropped.

## Shipped — v0.1.x (July 2026)

The release engineering formerly listed here as "v0.1.0 — first public
release" is done: public API audit with rustdoc on every public item
(`#![warn(missing_docs)]`), the crate published to crates.io via Trusted
Publishing, and abi3 Python wheels on PyPI built by maturin CI for
Linux/macOS/Windows. Rust and Python tests now run on all three platforms,
and CI verifies the declared Rust 1.90 minimum. Still open from that list:

- Demo assets: session-demo and playground recordings for the README (the
  dungeon demo video shipped with v0.1.2).

Shipped since the first release:

- **First-class diff** (v0.1.3) — the divergence of two timelines as an
  engine operation: a position plus three replay plans, computed from the
  branch catalog alone, no record comparison, no new durable state. Spec:
  [docs/specs/first-class-diff.md](docs/specs/first-class-diff.md); the
  chat demo's application-level `/diff` became its property-test oracle.
- **`db.watch`** (v0.1.3) — the committed-batch feed as a blocking Python
  iterator (`tail -f` for the log); binding-only over the existing feed.

## v0.x — after the first release

Ordered roughly by how directly each builds on shipped seams:

- **Background healing.** Instant recovery ships with the background-healer
  seam in place but disabled — cold partitions heal on first read only.
  Turning it on (idle-time healing, oldest-snapshot-first) is a performance
  feature that must never change an answer.
- **Retention and compaction — explicit-floor path shipped.**
  Persistent floors, typed
  unavailable errors, and non-destructive blocker-aware planning implement
  phases 1–2 of the normative
  [retention/compaction contract](docs/specs/retention-compaction.md). The
  engine can now publish and validate the phase-3 stream/branch core anchor,
  and facade projections can promote verified partition checkpoints into
  anchor format v2. Anchor format v3 also promotes opaque branch and durable
  consumer bootstraps at the effective floor. Atomic apply now binds the
  manifest generation to the anchor checksum before reclaiming whole closed
  segments. Protected-versus-disposable projection policy, consumer
  abandonment and richer plan diagnostics remain future work. Lagging feeds
  now have a scoped descriptor/fetch/resume bootstrap workflow. A deterministic
  seven-boundary retention crash matrix now runs in the Linux and Windows
  nightly rotation. Operators can inspect a read-only status surface covering
  identity, proposed progress, blockers, bootstrap readiness, open handles,
  reclaimable storage, and best-effort cleanup progress from Rust or Python.
  Exact-position, latest-event, timestamp-cutoff, and retained-byte policies
  now select positions without weakening the explicit-floor safety path.
- **Replication adapters.** The committed-batch feed is the replication
  seam: follower ingestion keyed by original event/batch identity is
  idempotent by construction. Adapters (file shipping, object storage, HTTP)
  stay out of the core engine.
- **MCP server.** Agent memory over the engine facade as a Model Context
  Protocol server, so any MCP-speaking agent gets durable, forkable,
  time-travelable memory without bindings.
- **Inspector (`salamander-scope`).** The playground
  (`salamander-demo -- ui`) is the seed; a real inspector adds projection
  state at any position (`view_at`), partition/heal status, snapshot
  catalogs, and feed consumers.
- **Self-describing payload codec.** Store JSON natively instead of the
  `Json` newtype's text round-trip — a payload-format version bump the
  format has reserved room for since WP-01.
- **More language bindings.** The non-generic facade and its DTO contract
  (WP-05) were designed for Node, Java, Go, and .NET; each binding is
  translation only, never semantics.

## Later phases

- **Phase 3 — concurrent producers.** Multiple in-process producer threads
  behind a sequencer feeding the single writer, batched through group
  commit. Still one logical writer; this changes who may call, not the
  ordering model.
- **Phase 4 — buffer-managed projection store.** A pointer-swizzling,
  larger-than-memory projection store for derived state that outgrows RAM.
  Research-grade; only after real workloads demand it.

## Demand-driven (no current plan)

Secondary-index persistence formats beyond snapshots, a query language, and
vector search are deliberately unplanned. The guiding principle is to add
structures only in response to benchmarked workloads. If your workload needs
one of these, an issue with numbers is the fastest way to move it up.

## Permanent non-goals

- **Multi-writer / multi-process shared state.** One logical writer per
  database, forever. Replication is a *reader* of one writer's log, not a
  merge of many.
- **Interpreting your payloads.** The engine orders and persists opaque
  bytes (INV-9). A flight recorder, not a flight instructor: garbage events
  replay faithfully as garbage.
