"""pytest suite for the query layer + fork (WP-5 steps 8b/8c).

    python -m pytest examples/py/test_query_and_fork.py -v
"""

from concurrent.futures import ThreadPoolExecutor

import pytest

import salamander


def test_register_view_get_and_index(tmp_path):
    db = salamander.open(str(tmp_path / "db"))
    db.register_view("events", key="id", indexes={"by_kind": "kind"})
    db.append("s", {"id": "a", "kind": "user"})
    db.append("s", {"id": "b", "kind": "tool"})
    db.append("s", {"id": "c", "kind": "user"})
    db.append("s", {"note": "no id — ignored by the view"})

    v = db.view("events")
    assert v.len() == 3
    assert v.get("a")["kind"] == "user"
    assert v.get("missing") is None
    assert len(v.by("by_kind", "user")) == 2
    assert len(v.by("by_kind", "tool")) == 1
    assert v.by("by_kind", "nope") == []


def test_view_range_and_prefix(tmp_path):
    db = salamander.open(str(tmp_path / "db"))
    db.register_view("kv", key="id")
    for name in ["k1", "k2", "k3", "user1"]:
        db.append("s", {"id": name})

    v = db.view("kv")
    assert [r["id"] for r in v.range("k1", "k3")] == ["k1", "k2"]  # [lo, hi)
    assert sorted(r["id"] for r in v.prefix("k")) == ["k1", "k2", "k3"]


def test_view_where_filter(tmp_path):
    db = salamander.open(str(tmp_path / "db"))
    db.register_view("calls", key="id", where_field="kind", where_value="tool_call")
    db.append("s", {"id": "t1", "kind": "tool_call"})
    db.append("s", {"id": "m1", "kind": "user_msg"})  # filtered out

    v = db.view("calls")
    assert v.len() == 1
    assert v.get("t1") is not None
    assert v.get("m1") is None


def test_view_updates_live_on_append(tmp_path):
    db = salamander.open(str(tmp_path / "db"))
    db.register_view("kv", key="id")
    v = db.view("kv")
    assert v.len() == 0
    db.append("s", {"id": "a", "v": 1})
    assert v.len() == 1            # the same handle sees the new row (INV-2)
    db.append("s", {"id": "a", "v": 2})   # overwrite
    assert v.get("a")["v"] == 2


def test_deregister_view(tmp_path):
    db = salamander.open(str(tmp_path / "db"))
    db.register_view("kv", key="id")
    assert db.deregister_view("kv") is True
    assert db.deregister_view("kv") is False
    with pytest.raises(KeyError):
        db.view("kv")


def test_fork_stitches_history_and_leaves_parent_untouched(tmp_path):
    db = salamander.open(str(tmp_path / "db"))
    db.append("chat", {"role": "user", "text": "hi"})
    db.append("chat", {"role": "assistant", "text": "how many days?"})
    at = db.head()
    db.append("chat", {"role": "user", "text": "5 days"})

    child = db.fork("chat", at)
    db.append_branch(child, "chat", {"role": "user", "text": "3 days"})

    # The engine branch replays the shared prefix, then its own turn.
    child_texts = [e["body"]["text"] for e in db.branch_history(child, "chat")]
    assert child_texts == ["hi", "how many days?", "3 days"]

    # The parent is exactly as recorded — the fork didn't touch it.
    parent_texts = [e["body"]["text"] for e in db.history("chat")]
    assert parent_texts == ["hi", "how many days?", "5 days"]


def test_fork_rejects_bad_offset_and_duplicate(tmp_path):
    db = salamander.open(str(tmp_path / "db"))
    db.append("chat", {"text": "a"})
    at = db.head()
    db.append("chat", {"text": "b"})

    with pytest.raises(ValueError):
        db.fork("chat", 999)  # beyond head

    db.fork("chat", at)  # first fork at this point is fine
    with pytest.raises(ValueError):
        db.fork("chat", at)  # same parent@offset again -> collision


def test_facade_receipt_paging_and_close(tmp_path):
    db = salamander.open(str(tmp_path / "db"))
    receipt = db.append_receipt("s", {"id": "a"})
    assert receipt["first_position"] == 0
    assert receipt["last_position"] == 0
    assert receipt["current_revision"] == 0
    assert len(receipt["batch_id"]) == 32

    db.append("s", {"id": "b"})
    reader = db.open_reader("s", page_events=1)
    first = reader.next_page()
    second = reader.next_page()
    assert [row["body"]["id"] for row in first["records"]] == ["a"]
    assert [row["body"]["id"] for row in second["records"]] == ["b"]
    assert first["continuation"] == 1
    assert second["done"] is True
    reader.close()
    with pytest.raises(RuntimeError):
        reader.next_page()

    db.close()
    with pytest.raises(RuntimeError):
        db.head()


def test_python_threads_share_the_safe_sequencer(tmp_path):
    db = salamander.open(str(tmp_path / "db"))

    def append_range(worker):
        for item in range(25):
            db.append("shared", {"worker": worker, "item": item})

    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(append_range, range(4)))

    assert db.head() == 100
