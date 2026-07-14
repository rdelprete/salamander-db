"""pytest suite for the unkillable-agent demo.

    python -m pytest examples/py/test_unkillable_agent.py -v

Most tests drive the demo's functions in-process (a caught exception plus a
close/reopen stands in for a process death); one integration test runs the
real parent/child subprocess cycle.
"""

import os
import subprocess
import sys

import pytest

import salamander
import unkillable_agent as ua

ALL_STEPS = [n for n, _, _ in ua.STEPS]


class Died(Exception):
    """Catchable stand-in for the demo's os._exit."""


def _die():
    raise Died


def _run_until_complete(path, kill_at):
    """The parent loop, in-process: run, 'die', reopen, repeat."""
    runs = 0
    while True:
        runs += 1
        assert runs <= len(ua.STEPS) + 2, "no progress across restarts"
        db = salamander.open(path)
        try:
            if ua.run_steps(db, kill_at=kill_at, die=_die):
                return db, runs
        except Died:
            pass
        db.close()


def test_resume_computation_is_a_pure_fold():
    assert ua.next_step(set()) == 1
    assert ua.next_step({1, 2, 3}) == 4


def test_parse_kill_at():
    assert ua.parse_kill_at("") == frozenset()
    assert ua.parse_kill_at("3,5") == frozenset({3, 5})


def test_happy_path_no_kills(tmp_path):
    db = salamander.open(str(tmp_path / "db"))
    assert ua.run_steps(db) is True
    assert ua.verify(db) == ALL_STEPS


def test_kill_and_resume_executes_each_step_exactly_once(tmp_path):
    db, runs = _run_until_complete(str(tmp_path / "db"), frozenset({3, 5}))
    assert runs == 3  # died at 3, died at 5, finished
    assert ua.verify(db) == ALL_STEPS


def test_kill_at_first_and_final_step(tmp_path):
    kill_at = frozenset({1, len(ua.STEPS)})
    db, runs = _run_until_complete(str(tmp_path / "db"), kill_at)
    assert runs == 3  # the last run finds everything done and just completes
    assert ua.verify(db) == ALL_STEPS


def test_verify_rejects_a_gap(tmp_path):
    db = salamander.open(str(tmp_path / "db"))
    for n, kind, detail in ua.STEPS:
        if n != 4:
            ua.append_step(db, n, kind, detail)
    with pytest.raises(AssertionError):
        ua.verify(db)


def test_idempotent_retry_returns_original_receipt(tmp_path):
    # The one crash window replay can't distinguish: the step was durable
    # but the worker died before acknowledging. The retry must be a no-op.
    db = salamander.open(str(tmp_path / "db"))
    n, kind, detail = ua.STEPS[0]
    first = ua.append_step(db, n, kind, detail)
    retry = ua.append_step(db, n, kind, detail)
    assert retry["batch_id"] == first["batch_id"]
    assert len(db.replay(ua.NAMESPACE)) == 1


def test_subprocess_parent_child_cycle(tmp_path):
    env = dict(os.environ)
    env["SALAMANDER_KILL_AT"] = "2,4"
    env["PYTHONIOENCODING"] = "utf-8"
    script = os.path.join(os.path.dirname(__file__), "unkillable_agent.py")
    result = subprocess.run(
        [sys.executable, script, str(tmp_path / "db")],
        env=env, capture_output=True, text=True, timeout=120,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.count("died: simulated crash") == 2
    assert "each executed exactly once" in result.stdout
