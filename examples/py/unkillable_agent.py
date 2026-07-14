"""The agent that survives its own murder.

A scripted agent works through a multi-step task. At scheduled points the
worker process dies hard — ``os._exit``: no cleanup, no goodbye. A parent
loop restarts it, and the restarted worker resumes exactly where the log
says it left off. The entire resume logic is two lines:

    done = {ev["body"]["step"] for ev in db.replay("task")}
    next_step = max(done, default=0) + 1

No checkpoint file, no recovery code, no step executed twice. Each step is
appended with ``durability="sync"`` (durable before it counts as done) and
an idempotency key (a retry of a durable-but-unacknowledged step returns
the original receipt instead of duplicating).

Run (after building the extension with `maturin develop`):

    python examples/py/unkillable_agent.py

Environment knobs, mostly for tests and recordings:

    SALAMANDER_KILL_AT=3,5   exact kill schedule (empty string = no kills)
    SALAMANDER_KILL_SEED=42  seed for the default random schedule

True torn-write / `kill -9` coverage is the Rust crash harness's job
(`cargo run -p salamander-demo -- crashtest parent`); this demo's claim is
resume with exactly-once steps.
"""

import os
import random
import shutil
import subprocess
import sys
import tempfile
import time

import salamander

NAMESPACE = "task"

# The same debugging fable as the Rust session demo, for continuity.
STEPS = [
    (1, "plan", "check logs, then recent deploys"),
    (2, "tool", "grep_logs -> NPE in CartValidator:88"),
    (3, "tool", "git_blame -> deploy #4213, 2h ago"),
    (4, "decision", "root cause is deploy #4213"),
    (5, "tool", "rollback_deploy -> rolled back to #4212"),
    (6, "verify", "checkout 500s stopped"),
    (7, "done", "resolved via rollback"),
]

KILLED_EXIT_CODE = 137
STEP_SLEEP_SECS = 0.15


def say(msg=""):
    # flush=True everywhere: the worker dies via os._exit, which skips
    # buffer flushing — narration must not die with it.
    print(msg, flush=True)


def completed_steps(db):
    """Which steps are already durable? The log is the only source."""
    return {ev["body"]["step"] for ev in db.replay(NAMESPACE)}


def next_step(done):
    """Steps run in order, so the resume point is one past the highest."""
    return max(done, default=0) + 1


def append_step(db, n, kind, detail):
    """One step = one atomic, fsynced batch with an idempotency key."""
    return db.append_batch(
        NAMESPACE,
        [{"body": {"step": n, "kind": kind, "detail": detail},
          "event_type": f"step.{kind}"}],
        idempotency_key=f"step-{n}",
        durability="sync",
    )


def run_steps(db, kill_at=frozenset(), die=None):
    """Execute the remaining steps; returns True when the task completed.

    ``die`` is how a scheduled crash happens — the demo passes os._exit,
    tests pass something catchable.
    """
    if die is None:
        die = lambda: os._exit(KILLED_EXIT_CODE)
    done = completed_steps(db)
    start = next_step(done)
    if done:
        span = f"step {max(done)}" if max(done) == 1 else f"steps 1-{max(done)}"
        say(f"  replay says {span} done, resuming at {start}")
    for n, kind, detail in STEPS[start - 1:]:
        time.sleep(STEP_SLEEP_SECS)
        append_step(db, n, kind, detail)
        say(f"  [{n}] {kind + ': ' + detail:<52} durable")
        if n in kill_at:
            say("  died: simulated crash -- no cleanup, no goodbye")
            die()
            return False  # only reachable with a non-exiting test `die`
    say("  task complete")
    return True


def verify(db):
    """The proof-of-claim: every step present exactly once, in order."""
    steps = [ev["body"]["step"] for ev in db.replay(NAMESPACE)]
    expected = [n for n, _, _ in STEPS]
    if steps != expected:
        raise AssertionError(f"expected steps {expected}, log has {steps}")
    return steps


def parse_kill_at(raw):
    return frozenset(int(s) for s in raw.split(",") if s.strip())


def pick_kill_schedule():
    """Two kill points, seeded and reproducible; env overrides win."""
    raw = os.environ.get("SALAMANDER_KILL_AT")
    if raw is not None:
        return parse_kill_at(raw), None
    seed = int(os.environ.get("SALAMANDER_KILL_SEED", random.randrange(10_000)))
    # Not the final step, so the last run has at least one step to perform.
    kill_at = frozenset(random.Random(seed).sample(range(1, len(STEPS)), 2))
    return kill_at, seed


def run_child(data_dir):
    db = salamander.open(data_dir)
    kill_at = parse_kill_at(os.environ.get("SALAMANDER_KILL_AT", ""))
    run_steps(db, kill_at=kill_at)


def run_parent(data_dir, kill_at):
    env = dict(os.environ)
    env["SALAMANDER_KILL_AT"] = ",".join(str(n) for n in sorted(kill_at))
    env.setdefault("PYTHONIOENCODING", "utf-8")

    runs = 0
    # Each kill point fires at most once (finished steps never re-execute),
    # so this bound is unreachable unless resume itself is broken.
    max_runs = len(STEPS) + 2
    while True:
        runs += 1
        say(f"\n> run {runs}: task \"diagnose the checkout 500\" in {data_dir}")
        code = subprocess.run(
            [sys.executable, os.path.abspath(__file__), "child", data_dir],
            env=env,
        ).returncode
        if code == 0:
            break
        if code != KILLED_EXIT_CODE:
            raise RuntimeError(f"worker failed with unexpected exit code {code}")
        if runs >= max_runs:
            raise RuntimeError("worker keeps dying without making progress")
        # The worker died before its lock guard could run; the supervisor
        # knows it is gone. Same cleanup as the Rust crash harness parent.
        try:
            os.remove(os.path.join(data_dir, "LOCK"))
        except FileNotFoundError:
            pass

    # The workers are gone; take the writer lock and check the claim.
    steps = verify(salamander.open(data_dir))
    say(f"\nOK {runs} processes, {runs - 1} crashes, {len(steps)} steps -- "
        f"each executed exactly once.")
    say("   No checkpoint file. No recovery code. The log IS the resume logic:")
    say('       done = {ev["body"]["step"] for ev in db.replay("task")}')
    say("       next_step = max(done, default=0) + 1")
    return runs


def main(argv):
    if len(argv) >= 2 and argv[1] == "child":
        run_child(argv[2])
        return

    data_dir = argv[1] if len(argv) >= 2 else os.path.join(
        tempfile.gettempdir(), "salamander-unkillable-agent")
    shutil.rmtree(data_dir, ignore_errors=True)  # fresh task each run

    kill_at, seed = pick_kill_schedule()
    say("SalamanderDB -- the agent that survives its own murder")
    schedule = ", ".join(str(n) for n in sorted(kill_at)) or "none"
    if seed is not None:
        say(f"kill schedule: steps {{{schedule}}} "
            f"(seed {seed}; set SALAMANDER_KILL_SEED to reproduce)")
    else:
        say(f"kill schedule: steps {{{schedule}}} (from SALAMANDER_KILL_AT)")
    run_parent(data_dir, kill_at)


if __name__ == "__main__":
    main(sys.argv)
