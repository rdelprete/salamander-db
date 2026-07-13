# salamander-db

**SQLite for event-sourced state — built first for agent memory.**

An embedded event-sourcing engine. The append-only log is the only durable
structure; everything else is a rebuildable projection. Replay, time-travel,
and fork are first-class: rewind any session to step N, branch it, and run
two futures against the same history.

Second story: **crash-proof application state.** The torn-tail rule makes
"state that never logically existed" impossible after a kill — and the full
history comes free.

```rust
use salamander::agent::{EventBody, KvProjection};
use salamander::{AgentDb, Projection};

let mut db = AgentDb::open("./mydata")?;
db.append("session-1", EventBody::Put { key: "k".into(), value: b"v".to_vec() })?;
db.commit()?;                                  // fsync; durable

// Everything else rebuilds from the log:
let kv: KvProjection = db.projection()?;       // full replay
let past: KvProjection = db.view_at(1)?;       // state as of offset 1 (time-travel)
```

## Features

- **Segmented append-only log** — CRC32C framing, torn-tail truncation on
  open, atomic manifest, single-writer lock.
- **Projections** — deterministic folds of the log; `KvProjection`,
  `SessionProjection`, or your own via the `Projection` trait.
- **Time-travel & fork** — `view_at(n)`; `fork(ns, n)` branches a session
  while the log stays linear.
- **Payload-generic** — `Salamander<B>` over any serde payload; a provided
  `agent` vocabulary, `JsonDb` for dynamic JSON, or bring your own enum.
- **Query layer** — live registered views with secondary indexes:
  `get` / `range` / `prefix` / `by`, maintained incrementally (never stale).
- **Group commit** — combinable byte/count/time commit policies.
- **Crash-tested** — a `kill -9` harness asserts the core invariant across
  thousands of crashes.

## Status

Phase 1 core + Phase 1.5 (payload-generic engine, query layer, group commit,
dynamic-JSON payloads) complete. Single-writer, embedded, in-memory
projections (persistence is Phase 2). Not multi-writer, not a SQL/query
language, not a vector store — by design.

Roadmap, changelog, and runnable examples:
<https://github.com/rdelprete/salamander-db>

## License

MIT
