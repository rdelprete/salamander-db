# salamander-db (Python)

Native Python bindings for [salamander-db](../) — **SQLite for event-sourced
state**. This is a compiled extension (via [PyO3](https://pyo3.rs) +
[maturin](https://www.maturin.rs)), so it embeds the engine *in your process*
exactly like Python's built-in `sqlite3` module wraps `libsqlite3`: you open
one handle and reuse it. No server, no subprocess.

The binding is a translation layer over the safe, non-generic Rust `Engine`.
Database ownership and cross-thread sequencing live in the core; Python does
not contain storage, replay, branching, or projection algorithms.

```python
import salamander

with salamander.open("./mem", commit_every_count=8) as db:
    db.append("session-1", {"kind": "user_msg", "text": "hi"})
    db.append("session-1", {"kind": "tool_call", "tool": "grep", "args": {"q": "500"}})
    db.commit()

    for ev in db.replay("session-1"):
        print(ev["offset"], ev["body"])      # plain dicts, nested fields intact
```

The open handle owns the single-writer lock, any registered views, and the
group-commit state — which is why it must be long-lived and in-process, not
re-created per call. Use it as a context manager or call `close()` explicitly
to release the lock deterministically.

The wheel includes a `py.typed` marker and `salamander.pyi`, so editors and
static type checkers can discover the public API and common dictionary shapes.
The runtime API is synchronous; its storage calls release the GIL, but asyncio
applications should use `asyncio.to_thread` or a dedicated worker rather than
block the event-loop thread. See the [Python usage guide](../docs/python-usage.md).

## Install

```bash
pip install salamander-db
```

Prebuilt abi3 wheels cover CPython 3.9+ on Linux (x86_64, aarch64), macOS
(Intel, Apple Silicon), and Windows (x86_64). The import name is
`salamander`.

## Build from source

Requires a Rust toolchain and Python ≥ 3.9.

```bash
python -m venv .venv && source .venv/bin/activate    # Windows: .venv\Scripts\activate
pip install maturin pytest
maturin develop -m salamander-py/Cargo.toml          # compiles + installs into the venv
python examples/py/quickstart.py
python -m pytest examples/py/test_roundtrip.py -v
```

On a very new Python that the pinned PyO3 doesn't recognize yet, prefix the
build with `PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1` (the abi3 stable ABI is
forward-compatible).

## API

| Python | Engine |
|---|---|
| `salamander.open(path, commit_every_bytes=, commit_every_count=, commit_every_millis=)` | `JsonDb::open_with_policy` |
| `db.append(namespace, event: dict) -> int` | `append` (payload = JSON) |
| `db.append_batch(namespace, events, ...) -> dict` | atomic batch append with concurrency, idempotency, and durability |
| `db.append_branch(branch, namespace, event) -> int` | append to an engine branch |
| `db.commit() -> int` | `commit` (fsync) |
| `db.head() -> int` | `head` |
| `db.uncommitted_count() -> int` | group-commit tally |
| `db.replay(namespace, start=0, end=None) -> list[dict]` | `replay` (literal) |
| `db.register_view(name, key, indexes={}, where_field=, where_value=)` | register an `IndexedView` |
| `db.view(name) -> View` | typed query handle |
| `db.deregister_view(name) -> bool` | `deregister` |
| `db.fork(namespace, at, parent=None) -> str` | create branch off `parent` (default: root timeline); forks of forks supported |
| `db.diff(left, right, namespace=, left_until=, right_until=) -> dict` | divergence of two timelines: common ancestor, exact divergence offset, and three pre-scoped readers (shared prefix + both suffixes) — computed from the branch catalog, no records compared |
| `db.watch(namespace=, branch=, start=, consumer_id=, timeout=) -> Watch` | blocking iterator over the committed-batch feed — `tail -f` for the log; yields events once durable |
| `db.history(namespace) -> list[dict]` | default-branch replay |
| `db.branch_history(branch, namespace) -> list[dict]` | inherited branch replay |

`View` handles support `.get(key)`, `.by(index, key)`, `.range(lo, hi)`,
`.prefix(p)`, `.len()`.

### Atomic batches and concurrency

`append_batch` exposes the engine's complete append contract. Every event is
a descriptor with a required `body` and optional `event_type`,
`schema_version`, `metadata` (byte strings or UTF-8 strings), and a 32-digit
hexadecimal `event_id`:

```python
receipt = db.append_batch(
    "orders",
    [
        {
            "body": {"order_id": "o-1", "state": "created"},
            "event_type": "order.created",
            "schema_version": 2,
            "metadata": {"trace_id": "trace-1"},
        },
        {
            "body": {"order_id": "o-1", "state": "paid"},
            "event_type": "order.paid",
        },
    ],
    expected_revision="no_stream",  # or "any" / an exact non-negative revision
    idempotency_key="create-o-1",   # bytes and UTF-8 strings are accepted
    durability="sync",              # "buffered" / "flush" / "sync"
)
```

The batch is visible all-or-nothing. Identical retries with the same
idempotency key return the original receipt; conflicting reuse raises
`salamander.ConflictError` and writes nothing. The other stable exception
categories include `InvalidArgumentError`, `NotFoundError`, `LockedError`,
`IoError`, `CorruptionError`, `UnsupportedFormatError`, `CodecError`,
`ResourceLimitError`, and `CancelledError`. They retain compatibility with
their former built-in bases (`ValueError`, `KeyError`, `OSError`, or
`RuntimeError`).

Pass `branch="branch-name"` to append the batch to an existing branch.
Optimistic revisions are branch-local: inherited history is replay-visible,
but the first local write to a stream on a new child branch uses
`expected_revision="no_stream"` (or `"any"`).
Replay rows include event and batch IDs, batch index, branch id, namespace,
stream revision, event type, schema version, codec, and metadata in addition
to `offset`, `timestamp_ms`, and `body`.

### Watching the log live

`db.watch` turns the engine's committed-batch feed into a blocking iterator
— build dashboards and bots on events as they land, not just after the
fact. Events are yielded only once **durable** (committed); the GIL is
released while waiting, and Ctrl+C stays responsive.

```python
watch = db.watch(namespace="jobs")        # live tail from the durable head
for ev in watch:                          # blocks until commits arrive
    handle(ev["body"])

db.watch(start=0)                         # full durable history, then follow
db.watch(branch="chat-fork-8")            # one timeline only
db.watch(timeout=5.0)                     # stop after 5 idle seconds

with db.watch(consumer_id="worker-1") as w:   # durable resume checkpoint
    for ev in w:
        process(ev)
        w.ack()          # a later watch with the same consumer_id resumes here
```

`start=None` (the default) tails from the durable head — or, with a
`consumer_id`, resumes from its last acknowledged checkpoint. A `timeout`
in seconds ends the iteration after that long without a matching event,
which is also what makes watches testable.

Payloads are any JSON-able Python value (`dict`/`list`/`str`/`int`/`float`/
`bool`/`None`), converted to/from `serde_json::Value` at the boundary. Views
are declared by field name (primary `key`, `indexes` mapping name→field, an
optional `where_field`/`where_value` filter) — no per-event Python callback
crosses the FFI boundary.

Demos ship in [`examples/py/`](../examples/py): `dungeon.py` (a browser
roguelike where rewind is a replay, dying is a fork, and a pull-the-plug
button proves the save can't corrupt), `chat.py` (a chat CLI where rewind,
fork, and timeline diff are storage primitives — Claude API when available,
offline mock otherwise), `unkillable_agent.py` (an agent hard-killed
mid-task that resumes from replay with exactly-once steps), and a LangGraph
checkpointer that survives process restarts. An MCP server is on the
[roadmap](../ROADMAP.md). Branch lifecycle and ancestry use the same engine
catalog as Rust:

```python
ancestry = db.branch_ancestry(branch_name)  # root-first metadata dictionaries
archived = db.archive_branch(branch_name)  # history remains readable
```

For version changes and on-disk compatibility, follow the
[upgrade guide](../docs/upgrading.md) before opening a production directory
with a newer release.
