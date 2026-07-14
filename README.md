# SalamanderDB

[![CI](https://github.com/rdelprete/salamander-db/actions/workflows/ci.yml/badge.svg)](https://github.com/rdelprete/salamander-db/actions/workflows/ci.yml)
[![Crash harness](https://github.com/rdelprete/salamander-db/actions/workflows/crash.yml/badge.svg)](https://github.com/rdelprete/salamander-db/actions/workflows/crash.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

**SQLite for event-sourced state — built first for agent memory.**

SalamanderDB is an embedded event-sourcing engine written in Rust. It runs
in your process like SQLite — no server, no subprocess — and stores your
application's state as an append-only log of events. Everything else
(key/value state, indexes, query views) is a projection folded from that
log, disposable and rebuildable by definition. Replay, time-travel, and
fork are first-class operations: rewind any session to step N, branch it,
and run two futures against the same history.

## Why

Mutable state destroys its own history. When an agent does something
inexplicable at step 40, when a process dies mid-write and leaves a
half-written checkpoint file, when someone asks "why is the state like
this?" — the information you need is exactly what an in-place update threw
away. SalamanderDB keeps it:

- **The log is the only durable structure.** A torn tail is truncated at
  the last valid record on open, so state that never logically existed is
  impossible. No corrupt-checkpoint recovery dance.
- **Projections are caches.** Dropping derived state is always safe;
  rebuilding it is always possible and deterministic — same events, same
  order, same state, every time.
- **History is queryable.** `view_at(n)` answers "what did the world look
  like at step n" directly from the record, not from a reconstructed
  timeline.
- **Fork is cheap.** Branch a session at any point and let two histories
  diverge — the postmortem that lets you replay an incident against a fix.

Built first for **agent memory** (typed events for tool calls, model turns,
decisions; a working LangGraph checkpointer), but the core is
payload-generic: the second use case is **crash-proof application state**
for any app that currently fears its own JSON or pickle checkpoint file.

## Quick start

Not yet on crates.io — build from source:

```
git clone https://github.com/rdelprete/salamander-db
cd salamander-db
cargo test --workspace
```

### Rust

```rust
use salamander::agent::{EventBody, KvProjection};
use salamander::{AgentDb, Projection};

fn main() -> salamander::Result<()> {
    let mut db = AgentDb::open("./mem")?;
    db.append("notes", EventBody::Put { key: "lang".into(), value: b"Rust".to_vec() })?;
    db.append("notes", EventBody::Delete { key: "title".into() })?;
    db.commit()?; // fsync — durable up to here

    // Derived state is a fold over the log, rebuilt on demand:
    let kv: KvProjection = db.projection()?;
    println!("{:?}", kv.state().get("lang"));

    // Time-travel: the same fold, stopped at offset 1:
    let past: KvProjection = db.view_at(1)?;
    println!("{:?}", past.state().get("title"));
    Ok(())
}
```

Runnable, commented examples live in
[`salamander/examples/`](salamander/examples) — key/value basics, custom
payload types, query views (`get`/`range`/`prefix`/`by`), forking, and JSON
payloads with commit policies:

```
cargo run --example 01_kv_basics -p salamander
```

### Python

SalamanderDB embeds in Python like `sqlite3` — a native extension, one
in-process handle:

```python
import salamander
db = salamander.open("./mem", commit_every_count=8)
db.append("session-1", {"kind": "user_msg", "text": "hi"})
for ev in db.replay("session-1"):
    print(ev["offset"], ev["body"])   # -> plain dicts back out
```

Build with maturin — see [`salamander-py/`](salamander-py) and
[`examples/py/`](examples/py), including a LangGraph checkpointer that
survives a process restart.

### Demos

**Try it in the browser** — the playground spins up a local database with
a web UI (no dependencies, no server framework — one `cargo run`):

```
cargo run -p salamander-demo -- ui        # http://127.0.0.1:7171
```

Append JSON events to named streams, scrub the time-travel slider back
through history, fork the timeline at any point, and switch between
branches to watch them diverge over a shared prefix. The directory
persists — reopen it later from the UI or the Rust/Python API.

The flagship terminal demo records a debugging session, forks it at the
root-cause decision, and shows both branches diverging over a shared
history:

```
cargo run -p salamander-demo -- session
```

<details>
<summary>What it prints — a debugging session, forked at the root-cause decision</summary>

```
SalamanderDB — session demo

▶ Recorded a debugging session under namespace "debug-session":

  [ 0] session started (agent "assistant-alpha")
  [ 1] user: The checkout page throws a 500 on submit. Can you find the cause?
  [ 2] assistant: Let me check the server logs and the recent deploys.
  [ 3] → call grep_logs(…)
  [ 4] ← ok NullPointerException in CartValidator.validate() at line 88
  [ 5] → call git_blame(…)
  [ 6] ← ok line 88 last changed in deploy #4213 (2h ago): 'skip null coupon check'
  [ 7] ★ decision: Root cause: deploy #4213 removed a null-check on coupon
      · · · · · · · · · ·  fork point (offset 8)  · · · · · · · · · ·
  [ 8] assistant: I'll roll back deploy #4213 to restore the null check.
  [ 9] → call rollback_deploy(…)
  [10] ← ok rolled back to #4212. Checkout 500s stopped.
  [11] session ended: resolved via rollback

▶ Forking at offset 8 (just after the root-cause decision)…
  new namespace: "debug-session-fork-8"  — it replays offsets 0..8, then diverges.

▶ Two branches, same first 7 transcript entries, then divergent:

  PARENT  debug-session                          FORK  debug-session-fork-8
  --------------------------------------------   --------------------------------------------
  user: The checkout page throws a 500 on sub…   user: The checkout page throws a 500 on sub…
  assistant: Let me check the server logs and…   assistant: Let me check the server logs and…
  → call grep_logs(…)                            → call grep_logs(…)
  ← ok NullPointerException in CartValidator.…   ← ok NullPointerException in CartValidator.…
  → call git_blame(…)                            → call git_blame(…)
  ← ok line 88 last changed in deploy #4213 (…   ← ok line 88 last changed in deploy #4213 (…
  ★ decision: Root cause: deploy #4213 remove…   ★ decision: Root cause: deploy #4213 remove…
  assistant: I'll roll back deploy #4213 to r…   assistant: Rather than roll back, I'll forw…  ◀ diverge
  → call rollback_deploy(…)                      → call open_pr(…)
  ← ok rolled back to #4212. Checkout 500s st…   ← ok PR #991 opened with the null guard res…
                                                 ★ decision: Forward-fix instead of rollback

  Both branches share the history before offset 8.
  The parent is untouched by the fork — its transcript is exactly as recorded.
  (17 events total across both namespaces; the log stayed linear.)
```

</details>

Or run the write storm, which appends a million events, reopens the
directory, and verifies every record survived:

```
cargo run --release -p salamander-demo -- storm   # 1,000,000 events (pass a count to change)
```

Python demos make the same points from the other side of the FFI (build the
extension first — see [`salamander-py/`](salamander-py)):

```
python examples/py/dungeon.py            # browser roguelike: rewind is a replay,
                                         # dying is a fork, the save can't corrupt
python examples/py/chat.py               # chat CLI: /rewind, /fork, /diff — the
                                         # "edit and regenerate" feature as storage
python examples/py/unkillable_agent.py   # an agent hard-killed twice mid-task;
                                         # resumes from replay, every step exactly once
```

**The Undying Dungeon** (`dungeon.py`) serves a one-page game at
`http://127.0.0.1:7172` with no dependencies beyond the standard library:
every move is an event, the timeline scrubber is `view_at(n)`, and when you
die you drag back and *fork* a new future over the shared past. A red button
pulls the plug — `os._exit` mid-write — and relaunching resumes exactly,
because the log is the only durable structure. The chat demo talks to the
Claude API when `ANTHROPIC_API_KEY` is set (and `anthropic` is installed),
falling back to a deterministic offline mock; its directory persists,
branches and all, across runs.

## Performance

The claim behind "instant recovery": **open time is independent of log and
projection size, and the first query costs what it touches, not what the
database holds.** Open loads catalogs only — no snapshot load, no event
replay; projection state heals partition-by-partition on first read from
verified snapshots plus a selective replay of just that partition's suffix.

For scale context, a representative single run on a dev laptop
(1,000,000 events, ~74 MiB on disk): log recovery on open is ~37 ms and
roughly flat, while a *full* eager projection rebuild — the cost instant
recovery replaces with lazy healing — is ~580 ms and linear in log size.
Measure it yourself:

```
cargo bench -p salamander-db --bench instant_recovery   # catalog-only open vs heal-1-of-64 vs heal-all
cargo bench -p salamander-db --bench replay             # full scan vs single-stream vs tail seek
SALAMANDER_BENCH_SIZES=1000000,10000000 cargo bench -p salamander-db --bench open_time
```

## Status and roadmap

**The architecture is complete.** The full post-v0.1 design — built on a set
of normative correctness contracts — is implemented and tested:

- **Format v2** — stable engine envelope over opaque payloads, golden byte
  fixtures, offline v1 migration
- **Streams and batches** — named streams with contiguous revisions,
  optimistic concurrency, atomic all-or-nothing batches, idempotent retries,
  explicit `Buffered`/`Flush`/`Sync` durability on every append
- **Branches** — durable engine-owned ancestry: fork at any batch boundary,
  isolated inherited replay, archival
- **Streaming reader** — declarative `ReplayPlan`s with bounded memory and
  derived segment-skip indexes
- **Engine facade** — a non-generic, thread-safe boundary with stable DTOs;
  the Python bindings (and future languages) are translation only
- **Projection runtime** — durable versioned descriptors, isolated failure,
  `RequireHead`/`AllowStale`/`WaitFor` query consistency
- **Verified snapshots** — atomic, checksummed, treated as hostile cache
  input; deleting all derived state changes performance only, never answers
- **Committed-batch feed** — a bounded, resumable subscription/replication
  primitive over durable batches
- **Instant recovery** — open loads catalogs only; partitioned projection
  state heals lazily on first read, Graefe-style

Everything is defended by the workspace test suite (unit, property-based,
golden-fixture, and integration tests) and a `kill -9` crash harness; see
[CHANGELOG.md](CHANGELOG.md) for the full feature inventory.

**What's next** — release engineering toward `v0.1.0` (crates.io, PyPI
wheels, CI platform matrix), then background healing, a retention contract,
replication adapters, and an MCP server. Permanently out of scope:
multi-writer shared state across services. See [ROADMAP.md](ROADMAP.md).

## Documentation

| Doc | What it covers |
|---|---|
| [ROADMAP.md](ROADMAP.md) | What's next, what's demand-driven, what's permanently out of scope |
| [CHANGELOG.md](CHANGELOG.md) | The full feature inventory of the current release |
| [CONTRIBUTING.md](CONTRIBUTING.md) | Ground rules, the CI gates, and how to propose changes |
| [salamander/examples/](salamander/examples) | Runnable, commented Rust examples for every core operation |
| [examples/py/](examples/py) | Python examples, including the LangGraph checkpointer |

## License

MIT. Single-writer, embedded, by design.
