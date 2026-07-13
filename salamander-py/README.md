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

db = salamander.open("./mem", commit_every_count=8)   # one in-process handle
db.append("session-1", {"kind": "user_msg", "text": "hi"})
db.append("session-1", {"kind": "tool_call", "tool": "grep", "args": {"q": "500"}})
db.commit()

for ev in db.replay("session-1"):
    print(ev["offset"], ev["body"])          # -> plain dicts, nested fields intact
```

The open handle owns the single-writer lock, any registered views, and the
group-commit state — which is why it must be long-lived and in-process, not
re-created per call.

## Build it

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
| `db.fork(namespace, at) -> str` | create branch (returns branch name) |
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
Replay rows include event and batch IDs, batch index, stream revision, event
type, schema version, codec, and metadata in addition to `offset`,
`timestamp_ms`, and `body`.

Payloads are any JSON-able Python value (`dict`/`list`/`str`/`int`/`float`/
`bool`/`None`), converted to/from `serde_json::Value` at the boundary. Views
are declared by field name (primary `key`, `indexes` mapping name→field, an
optional `where_field`/`where_value` filter) — no per-event Python callback
crosses the FFI boundary.

A LangGraph checkpointer that survives process restarts ships in
[`examples/py/`](../examples/py); an MCP server is on the
[roadmap](../ROADMAP.md). Branch lifecycle and ancestry use the same engine
catalog as Rust:

```python
ancestry = db.branch_ancestry(branch_name)  # root-first metadata dictionaries
archived = db.archive_branch(branch_name)  # history remains readable
```
