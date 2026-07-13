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
