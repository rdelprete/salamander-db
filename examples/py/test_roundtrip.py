"""pytest suite for the native `salamander` extension (WP-5 step 8a exit).

Run (after `maturin develop`, with pytest installed):

    python -m pytest examples/py/test_roundtrip.py -v
"""

import pytest

import salamander


def test_append_replay_roundtrip(tmp_path):
    db = salamander.open(str(tmp_path / "db"))
    db.append("s", {"kind": "user_msg", "text": "hi"})
    db.append("s", {"kind": "decision", "summary": "ship it"})
    db.commit()

    rows = db.replay("s")
    assert [r["offset"] for r in rows] == [0, 1]
    assert rows[0]["body"] == {"kind": "user_msg", "text": "hi"}
    assert rows[1]["body"]["summary"] == "ship it"


def test_survives_reopen(tmp_path):
    path = str(tmp_path / "db")
    db = salamander.open(path)
    db.append("s", {"n": 1})
    db.append("s", {"n": 2})
    db.commit()
    del db  # release the single-writer lock

    reopened = salamander.open(path)
    assert reopened.head() == 2
    assert [r["body"]["n"] for r in reopened.replay("s")] == [1, 2]


def test_database_context_manager_closes_handle(tmp_path):
    path = str(tmp_path / "context-manager")
    with salamander.open(path) as db:
        db.append("session", {"kind": "started"})
        db.commit()

    with pytest.raises(RuntimeError):
        db.head()

    # Closing the first handle releases the single-writer lock.
    with salamander.open(path) as reopened:
        assert reopened.replay("session")[0]["body"] == {"kind": "started"}


def test_group_commit_policy(tmp_path):
    db = salamander.open(str(tmp_path / "db"), commit_every_count=2)
    db.append("s", {"n": 1})
    assert db.uncommitted_count() == 1
    db.append("s", {"n": 2})
    assert db.uncommitted_count() == 0  # count=2 policy fsynced


def test_json_types_roundtrip(tmp_path):
    db = salamander.open(str(tmp_path / "db"))
    payload = {
        "str": "text",
        "int": 42,
        "float": 3.5,
        "bool": True,
        "none": None,
        "list": [1, "two", False],
        "nested": {"a": {"b": [1, 2, 3]}},
    }
    db.append("s", payload)
    db.commit()

    (row,) = db.replay("s")
    assert row["body"] == payload  # exact round-trip through JSON


def test_single_writer_lock(tmp_path):
    path = str(tmp_path / "db")
    held = salamander.open(path)  # noqa: F841 — keep the lock held
    with pytest.raises(OSError):
        salamander.open(path)  # a second concurrent opener is refused


def test_replay_range(tmp_path):
    db = salamander.open(str(tmp_path / "db"))
    for i in range(5):
        db.append("s", {"i": i})
    db.commit()

    # [start, end) — offsets 1 and 2 only.
    rows = db.replay("s", start=1, end=3)
    assert [r["body"]["i"] for r in rows] == [1, 2]


def test_atomic_batch_exposes_full_event_and_receipt_semantics(tmp_path):
    db = salamander.open(str(tmp_path / "db"))
    receipt = db.append_batch(
        "orders",
        [
            {
                "body": {"order_id": "o-1", "state": "created"},
                "event_type": "order.created",
                "schema_version": 2,
                "metadata": {"trace_id": "trace-1", "binary": b"\x00\xff"},
                "event_id": "00000000000000000000000000000001",
            },
            {
                "body": {"order_id": "o-1", "state": "paid"},
                "event_type": "order.paid",
            },
        ],
        expected_revision="no_stream",
        idempotency_key="create-o-1",
        durability="sync",
    )

    assert receipt["first_position"] == 0
    assert receipt["last_position"] == 1
    assert receipt["previous_revision"] is None
    assert receipt["current_revision"] == 1
    assert receipt["durability"] == "synced"

    rows = db.replay("orders")
    assert [row["body"]["state"] for row in rows] == ["created", "paid"]
    assert [row["batch_index"] for row in rows] == [0, 1]
    assert [row["stream_revision"] for row in rows] == [0, 1]
    assert rows[0]["event_id"] == "00000000000000000000000000000001"
    assert rows[0]["event_type"] == "order.created"
    assert rows[0]["schema_version"] == 2
    assert rows[0]["codec"] == "json"
    assert rows[0]["metadata"]["trace_id"] == b"trace-1"
    assert rows[0]["metadata"]["binary"] == b"\x00\xff"
    assert rows[0]["batch_id"] == rows[1]["batch_id"]


def test_batch_revision_conflict_is_typed_and_writes_nothing(tmp_path):
    db = salamander.open(str(tmp_path / "db"))
    db.append_batch("s", [{"body": {"n": 1}}], expected_revision="no_stream")

    with pytest.raises(salamander.ConflictError):
        db.append_batch("s", [{"body": {"n": 2}}], expected_revision="no_stream")

    assert db.head() == 1
    assert [row["body"] for row in db.replay("s")] == [{"n": 1}]


def test_batch_idempotent_retry_returns_original_receipt(tmp_path):
    db = salamander.open(str(tmp_path / "db"))
    events = [{"body": {"n": 1}, "event_type": "counter.incremented"}]
    first = db.append_batch("s", events, idempotency_key=b"request-1")
    retry = db.append_batch("s", events, idempotency_key=b"request-1")

    assert retry == first
    assert db.head() == 1

    with pytest.raises(salamander.ConflictError):
        db.append_batch("s", [{"body": {"n": 2}}], idempotency_key=b"request-1")
    assert db.head() == 1


def test_exact_revision_and_branch_local_revision(tmp_path):
    db = salamander.open(str(tmp_path / "db"))
    db.append_batch("s", [{"body": {"n": 1}}], expected_revision="no_stream")
    receipt = db.append_batch(
        "s", [{"body": {"n": 2}}], expected_revision=0, durability="flush"
    )
    assert receipt["current_revision"] == 1
    assert receipt["durability"] == "flushed"

    child = db.fork("s", db.head())
    child_receipt = db.append_batch(
        "s", [{"body": {"n": 3}}], branch=child, expected_revision="no_stream"
    )
    assert child_receipt["current_revision"] == 0
    assert [row["body"]["n"] for row in db.branch_history(child, "s")] == [1, 2, 3]

@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"expected_revision": True}, "expected_revision"),
        ({"durability": "eventually"}, "durability"),
    ],
)
def test_batch_options_are_validated_before_write(tmp_path, kwargs, message):
    db = salamander.open(str(tmp_path / "db"))
    with pytest.raises(salamander.InvalidArgumentError, match=message):
        db.append_batch("s", [{"body": {"n": 1}}], **kwargs)
    assert db.head() == 0
