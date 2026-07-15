# Python usage

The `salamander-db` distribution installs the native `salamander` module.
It supports CPython 3.9 and newer and includes type information for editors
and static type checkers.

## Database lifetime

Keep one database handle open for the lifetime of the component that owns the
database. The handle owns the single-writer lock, registered views, and commit
policy state. Prefer a context manager so the lock is released deterministically:

```python
import salamander

with salamander.open("./agent-state", commit_every_count=8) as db:
    db.append("thread-1", {"kind": "user_message", "text": "hello"})
    db.commit()
```

Calling `close()` is equivalent when a context manager is inconvenient. Do not
open a new handle for every append, and do not expect two processes to write the
same database. Operations on a closed handle raise a typed Salamander exception.

`commit()` makes buffered appends durable. For an individual atomic batch, pass
`durability="sync"` when its receipt must represent an fsync before returning.

## Replay, readers, and watches

`replay()` materializes the selected history as a list. Use `open_reader()` for
bounded-memory paging over large histories. Reader pages contain `records`, a
`continuation` position, and `done`.

`watch()` is a blocking iterator over events after they become durable. A watch
with a `consumer_id` can call `ack()` to persist its resume position. Use the
watch as a context manager so its feed handle closes promptly.

```python
with salamander.open("./agent-state") as db:
    with db.watch(namespace="thread-1", start=0, timeout=5.0) as events:
        for event in events:
            print(event["body"])
```

## Async applications

The binding currently exposes a synchronous API. Storage calls release the
Python GIL while Rust performs I/O, but calling a blocking method directly from
an asyncio event-loop thread can still delay other tasks. Run calls in a worker
thread:

```python
import asyncio
import salamander

async def append_event(db: salamander.Salamander, event: object) -> int:
    return await asyncio.to_thread(db.append, "thread-1", event)
```

Keep ownership simple: use one long-lived handle and route database work through
a dedicated worker or a controlled `asyncio.to_thread` boundary. Native async
methods are not part of the v0.1 API.

## API discovery and errors

The wheel includes `salamander.pyi`, so editors expose method signatures and
return shapes. Event bodies accept JSON-compatible Python values. Replay rows,
receipts, branch records, and snapshot records are typed dictionaries in the
stub while remaining ordinary dictionaries at runtime.

Catch the narrowest stable exception category that applies, such as
`ConflictError`, `LockedError`, `CorruptionError`, or `ResourceLimitError`.
All library-defined categories remain compatible with their documented built-in
bases. See the [Python package reference](../salamander-py/README.md) for the
complete method inventory and examples.

## LangGraph

The repository includes a working checkpointer and restart demonstration in
[`examples/py/salamander_langgraph.py`](../examples/py/salamander_langgraph.py)
and [`examples/py/langgraph_demo.py`](../examples/py/langgraph_demo.py). The
integration is currently an example rather than a separately versioned adapter;
pin and test it with the LangGraph version used by your application.
