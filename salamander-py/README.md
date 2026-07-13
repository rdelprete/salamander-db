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
