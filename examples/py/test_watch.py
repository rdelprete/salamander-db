"""pytest suite for db.watch — the committed-batch feed as `tail -f`.

    python -m pytest examples/py/test_watch.py -v

A watch yields events only once they are *durable* (committed), so every
producer below commits explicitly. Timeouts are short: `timeout` ends the
iteration after that long without a matching event, which is what makes a
blocking iterator testable.
"""

import threading
import time

import pytest

import salamander


def test_live_tail_yields_only_events_committed_after_open(tmp_path):
    db = salamander.open(str(tmp_path / "db"))
    db.append("chat", {"text": "before"})
    db.commit()

    watch = db.watch(timeout=0.3)
    db.append("chat", {"text": "after-1"})
    db.append("chat", {"text": "after-2"})
    db.commit()

    texts = [ev["body"]["text"] for ev in watch]
    assert texts == ["after-1", "after-2"]


def test_start_zero_replays_history_then_follows(tmp_path):
    db = salamander.open(str(tmp_path / "db"))
    db.append("chat", {"text": "old"})
    db.commit()

    watch = db.watch(start=0, timeout=0.3)
    first = next(watch)
    assert first["body"]["text"] == "old"

    db.append("chat", {"text": "new"})
    db.commit()
    assert next(watch)["body"]["text"] == "new"
    assert list(watch) == []  # idle past the timeout -> iteration ends


def test_uncommitted_events_are_not_delivered(tmp_path):
    db = salamander.open(str(tmp_path / "db"))
    watch = db.watch(timeout=0.3)
    db.append("chat", {"text": "buffered"})  # appended, not yet durable

    assert list(watch) == []
    db.commit()
    assert [ev["body"]["text"] for ev in db.watch(start=0, timeout=0.3)] == [
        "buffered"
    ]


def test_namespace_filter_and_row_shape(tmp_path):
    db = salamander.open(str(tmp_path / "db"))
    watch = db.watch(namespace="metrics", timeout=0.3)
    db.append("chat", {"text": "noise"})
    db.append("metrics", {"cpu": 0.7})
    db.commit()

    rows = list(watch)
    assert len(rows) == 1
    assert rows[0]["namespace"] == "metrics"
    assert rows[0]["body"] == {"cpu": 0.7}
    assert rows[0]["offset"] == 1
    assert len(rows[0]["branch_id"]) == 32  # rows now say which branch


def test_branch_filter_follows_one_timeline(tmp_path):
    db = salamander.open(str(tmp_path / "db"))
    db.append("chat", {"text": "shared"})
    db.commit()
    child = db.fork("chat", db.head())

    main_watch = db.watch(branch="main", timeout=0.3)
    child_watch = db.watch(branch=child, timeout=0.3)
    db.append("chat", {"text": "on-main"})
    db.append_branch(child, "chat", {"text": "on-child"})
    db.commit()

    assert [ev["body"]["text"] for ev in main_watch] == ["on-main"]
    assert [ev["body"]["text"] for ev in child_watch] == ["on-child"]


def test_consumer_checkpoint_resumes_after_ack(tmp_path):
    db = salamander.open(str(tmp_path / "db"))
    for n in range(3):
        db.append("jobs", {"job": n})
    db.commit()

    watch = db.watch(start=0, consumer_id="worker-1", timeout=0.3)
    assert [ev["body"]["job"] for ev in watch] == [0, 1, 2]
    watch.ack()  # checkpoint: everything so far is processed
    watch.close()

    db.append("jobs", {"job": 3})
    db.commit()
    resumed = db.watch(consumer_id="worker-1", timeout=0.3)
    assert [ev["body"]["job"] for ev in resumed] == [3]


def test_blocking_wait_wakes_on_commit_from_another_thread(tmp_path):
    db = salamander.open(str(tmp_path / "db"))
    watch = db.watch(timeout=5.0)

    def produce():
        time.sleep(0.2)
        db.append("chat", {"text": "wake up"})
        db.commit()

    producer = threading.Thread(target=produce)
    started = time.monotonic()
    producer.start()
    event = next(watch)  # blocks (GIL released) until the commit lands
    elapsed = time.monotonic() - started
    producer.join()

    assert event["body"]["text"] == "wake up"
    assert elapsed < 5.0  # woke on the commit signal, not the timeout


def test_close_and_context_manager(tmp_path):
    db = salamander.open(str(tmp_path / "db"))
    watch = db.watch(timeout=0.1)
    watch.close()
    with pytest.raises(RuntimeError):
        next(watch)

    with db.watch(timeout=0.1) as scoped:
        assert list(scoped) == []
    with pytest.raises(RuntimeError):
        next(scoped)


def test_unknown_branch_raises_not_found(tmp_path):
    db = salamander.open(str(tmp_path / "db"))
    with pytest.raises(salamander.NotFoundError):
        db.watch(branch="no-such-branch")
