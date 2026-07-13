"""SalamanderDB from Python — the embedded, in-process model, exactly like
``sqlite3``: open ONE handle and reuse it. That handle holds the single-writer
lock and the group-commit state in-process; there is no server and no
subprocess.

Run (after building the extension with `maturin develop`):

    python examples/py/quickstart.py
"""

import os
import shutil
import tempfile

import salamander

print("salamander extension version:", salamander.__version__)

path = os.path.join(tempfile.gettempdir(), "salamander-py-quickstart")
shutil.rmtree(path, ignore_errors=True)  # fresh dir each run
print("data dir:", path, "\n")

# One handle, reused. Auto-commit every 4 events (group commit) — commit() is
# still available whenever you want an explicit fsync.
db = salamander.open(path, commit_every_count=4)
print(db)  # <salamander.Salamander head=0>

# Record an agent session as JSON events — plain Python dicts, nested and all.
db.append("session-1", {"kind": "user_msg", "text": "find the checkout bug"})
db.append("session-1", {"kind": "tool_call", "tool": "grep", "args": {"q": "500"}})
db.append("session-1", {"kind": "tool_result", "ok": True, "matches": 3})
db.append("session-1", {"kind": "decision", "summary": "null coupon check"})
print("uncommitted after 4 appends:", db.uncommitted_count(), "(the count=4 policy fsynced)")
print("head:", db.head())

print("\nreplay session-1:")
for ev in db.replay("session-1"):
    print(f"  [{ev['offset']}] {ev['body']}")

# The database IS the directory. Drop the handle (releasing the writer lock),
# reopen from cold, and the state is intact — the log is the only durable
# structure, everything else is a replay.
del db
db2 = salamander.open(path)
print("\nafter reopen, head =", db2.head())
kinds = [ev["body"]["kind"] for ev in db2.replay("session-1")]
print("kinds recovered:", kinds)

# A tool_call payload's nested field, straight back out as a dict:
tool_calls = [ev["body"] for ev in db2.replay("session-1") if ev["body"]["kind"] == "tool_call"]
print("first tool_call args:", tool_calls[0]["args"])
