# SalamanderDB Roadmap

**Where things stand (July 2026):** the post-v0.1 architecture is fully
implemented — from the stable v2 format through streams, branches, the
streaming reader, the engine facade, the projection runtime, verified
snapshots, the commit feed, and instant recovery — and the first public
releases are out: `salamander-db` on
[crates.io](https://crates.io/crates/salamander-db) and Python wheels on
[PyPI](https://pypi.org/project/salamander-db/), tagged `v0.1.0` through
`v0.1.3`. See [CHANGELOG.md](CHANGELOG.md) for the full feature inventory.

This file is direction, not commitment: unreleased items may change or be
dropped.

## Shipped — v0.1.x (July 2026)

The release engineering formerly listed here as "v0.1.0 — first public
release" is done: public API audit with rustdoc on every public item
(`#![warn(missing_docs)]`), the crate published to crates.io via Trusted
Publishing, and abi3 Python wheels on PyPI built by maturin CI for
Linux/macOS/Windows. Still open from that list:

- CI *test* matrix across Linux/macOS/Windows plus an MSRV job, so the
  durability platform matrix in the contracts is backed by tested platforms
  (tests currently run on Linux; wheels build on all three).
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
- **Retention and compaction.** Today the complete log is retained forever —
  that is what makes every derived structure disposable. A ratified
  retention contract comes before any byte is deleted; the commit feed
  already reserves the `PositionUnavailable` response and a
  bootstrap-from-snapshot protocol for this.
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
