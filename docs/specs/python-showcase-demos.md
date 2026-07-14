# Spec: Python showcase demos — `chat.py` and `unkillable_agent.py`

**Status:** implemented (see Implementation notes at the end)
**Target:** pre-`v0.1.0` demo assets ([ROADMAP.md](../../ROADMAP.md))
**Location:** both demos live in [`examples/py/`](../../examples/py) as single files,
alongside pytest suites that run in CI without network access.

## Motivation

The existing Python examples prove the API works (quickstart, LangGraph
checkpointer, pytest roundtrips) but none makes a visitor *feel* the pitch.
The Rust session demo is the flagship showcase; Python — the audience the
"agent memory" positioning actually targets — has no equivalent. These two
demos split the pitch:

- **`chat.py`** showcases *fork + time-travel* (the differentiator): the
  "edit an earlier message and regenerate" feature every chat product has,
  built in one file because the storage engine natively does it.
- **`unkillable_agent.py`** showcases *durability + idempotent resume* (the
  trust-builder): an agent that dies mid-task twice and still finishes,
  executing every step exactly once.

Both must run in under a minute with zero configuration and no network, and
both should record well for the README (`v0.1.0` demo-assets item).

## Shared constraints

- **Single file each.** No package, no framework. The demo *is* the
  documentation; comment density follows `quickstart.py`.
- **No new required dependencies.** `chat.py` may optionally use the
  `anthropic` package if installed *and* `ANTHROPIC_API_KEY` is set;
  otherwise it falls back to a deterministic mock model. CI always runs the
  mock path. `unkillable_agent.py` uses stdlib only.
- **Deterministic under test.** Every stochastic choice (mock replies, kill
  points) accepts a seed/env override so the pytest suites are stable.
- **The log is the only durable structure** (AGENTS.md invariant). Neither
  demo writes any state file of its own — resume, rewind, and branch
  switching are all replays. If a demo is tempted to persist something
  outside the engine, the design is wrong.

---

## Demo 1 — `chat.py`: a chat CLI with `/rewind` and `/fork`

### Pitch (what the recording shows)

```
$ python examples/py/chat.py ./chat-mem
you> what should I name my key/value store?
bot> How about something amphibious?
you> why amphibious?
bot> It survives on land and in water — like your data survives crashes.
you> /rewind 2                # scrub back: show the world as of offset 2
you> /fork                    # branch here; original timeline untouched
you> what about a bird name instead?
bot> Kestrel: small, fast, watches everything.
you> /branches                # two timelines, shared prefix, diverged
you> /switch main             # the original conversation, exactly as recorded
```

Then quit, re-run with the same directory, and the conversation — all
branches — is still there. The closing line of the demo prints the point:
*the edit-and-regenerate feature is ~150 lines because fork is a storage
primitive, not an application feature.*

### Event model

One namespace per conversation (default `"chat"`). Every turn is one event;
the transcript is a replay, the model's context window is a projection of it.

```python
{"role": "user",      "text": str}
{"role": "assistant", "text": str, "model": str}   # "mock" or the API model id
```

Appends use `db.append` (main timeline) / `db.append_branch` (on a branch)
with the default commit policy plus an explicit `db.commit()` after each
assistant turn — a user's turn and its reply become durable together.

### Session state

A `ChatSession` class owns the `salamander.open` handle and one piece of
in-memory state: the **current branch** (`None` = main timeline). Everything
else is derived:

- transcript: `db.history(ns)` or `db.branch_history(branch, ns)`
- model context: the transcript, truncated to the last N turns
- branch list: engine branch catalog (`db.branch_ancestry` for lineage
  display), discovered per command — never cached across commands

The CLI (`main()`) is a thin REPL over `ChatSession` so the pytest suite
drives the class directly, no stdin games.

### Commands

| Command | Behavior |
|---|---|
| *(free text)* | Append user event, call model with replayed context, append + print reply. |
| `/history` | Replay and print the current branch's transcript with offsets. |
| `/rewind N` | Print the transcript **as of offset N** (`db.replay(ns, end=N)` on main; list slice of `branch_history` on a branch). Read-only — nothing is destroyed. Remembers N as the pending fork point. |
| `/fork [N]` | `db.fork(ns, at)` at N (or the pending rewind point, or current head). Switch to the new branch. On the offset-collision `ValueError` (same parent@offset forked twice), switch to a fresh fork made at the same content point via a retry policy defined below. |
| `/branches` | List branches root-first with fork offsets and a side-by-side diverge marker, in the style of the Rust session demo. |
| `/switch NAME` | Set current branch (`main` = default timeline). |
| `/quit` | Commit and close. |

Fork-collision policy: the engine deliberately rejects a second fork at the
same parent@offset. The demo surfaces this honestly — it prints the existing
branch name and switches to it instead of forking, teaching the semantics
rather than papering over them.

**v1 limitation:** forking *from a branch* is out of scope — `db.fork`
targets the default timeline. `/fork` while on a branch prints a friendly
"switch to main to fork" message. (Revisit if/when the binding exposes
branch-of-branch forks.)

### Model backends

```python
class MockModel:   # default; deterministic, seeded, no network
class ClaudeModel: # iff `anthropic` importable and ANTHROPIC_API_KEY set
```

- `MockModel` produces short canned-but-varied replies from a seeded RNG and
  echoes enough of the user text to make transcripts readable. It must be
  fun enough that the no-API-key recording is still compelling.
- `ClaudeModel` calls the Messages API (`claude-haiku-4-5` default,
  overridable via `SALAMANDER_CHAT_MODEL`) with the replayed transcript as
  context — demonstrating that "context window = projection of the log".
- Backend selection is automatic with an explicit `--mock` override flag.
  The selected backend is printed at startup so recordings are unambiguous.

### CLI

```
python examples/py/chat.py [DATA_DIR] [--mock] [--ns NAME]
```

`DATA_DIR` defaults to `./chat-mem` and **persists** (unlike quickstart's
fresh-tempdir pattern) — reopening is part of the demo.

### Tests — `examples/py/test_chat.py`

All against `ChatSession` with `MockModel(seed=…)` and `tmp_path`:

1. Turn roundtrip: user + assistant events land with the right shape; commit
   happened (reopen and replay sees both).
2. `/rewind N` returns exactly the first N events and mutates nothing.
3. Fork at N, diverge, and assert (a) branch transcript = shared prefix +
   new suffix, (b) main transcript byte-identical to before the fork.
4. Fork collision: second fork at the same offset switches to the existing
   branch, no exception escapes.
5. Switch between main and branch; context sent to the model matches the
   current branch's replay.
6. Cold restart: new `ChatSession` over the same dir sees all branches.

---

## Demo 2 — `unkillable_agent.py`: the agent that survives its own murder

### Pitch (what the recording shows)

```
$ python examples/py/unkillable_agent.py
▶ run 1: starting task "diagnose the checkout 500"
  [1] plan: check logs, then recent deploys        ✓ durable
  [2] tool grep_logs → NPE in CartValidator:88     ✓ durable
  [3] tool git_blame → deploy #4213, 2h ago        ✓ durable
  💀 killed (simulated crash — no cleanup, no goodbye)
▶ run 2: reopened ./agent-mem — replay says steps 1–3 done, resuming at 4
  [4] decision: root cause is deploy #4213         ✓ durable
  [5] tool rollback_deploy → rolled back to #4212  ✓ durable
  💀 killed
▶ run 3: resuming at step 6
  [6] verify: checkout 500s stopped
  [7] done: resolved via rollback
✔ 3 processes, 2 crashes, 7 steps — each executed exactly once.
  No checkpoint file. No recovery code. The log IS the resume logic.
```

### Architecture: self-supervising, like the Rust crash harness

One file, two roles, mirroring `salamander-demo -- crashtest parent`:

- **Parent** (default entrypoint): loops `subprocess.run([sys.executable,
  __file__, "child", data_dir])` until the child exits 0, narrating each
  death and restart. Passes the kill schedule via env.
- **Child**: opens the db, replays the task stream to find the last
  completed step, executes remaining steps, and — when it hits a scheduled
  kill point — dies via `os._exit(137)`: no `atexit`, no buffer flushing, no
  destructor. (True torn-write/`kill -9` coverage belongs to the Rust crash
  harness; this demo's claim is *resume with exactly-once steps*, and the
  demo says so in a comment rather than overclaiming.)

Kill schedule: by default two kill points drawn from a seeded RNG (seed
printed, overridable via `SALAMANDER_KILL_SEED`); `SALAMANDER_KILL_AT`
(comma-separated step numbers) sets it exactly, which is what the tests use.
The parent enforces a max-restarts bound (steps + 2) so a logic bug can
never loop forever.

### The task and its event model

A fixed scripted task — the same debugging fable as the Rust session demo,
for cross-language continuity. Namespace `"task"`. Each step is appended
with the engine's full contract, which is the point of the demo:

```python
db.append_batch(
    "task",
    [{"body": {"step": n, "kind": ..., "detail": ...},
      "event_type": f"step.{kind}"}],
    idempotency_key=f"step-{n}",   # crash between fsync and "ack" ⇒ retry is a no-op
    durability="sync",             # durable before the step is considered done
)
```

Steps simulate work with a short `time.sleep`, long enough that recordings
read as "real work" but the whole demo stays well under a minute.

### Resume logic (the whole payload of the demo)

```python
done = {ev["body"]["step"] for ev in db.replay("task")}
next_step = max(done, default=0) + 1
```

That's it — and the demo prints exactly those two lines of code in its
narration, because "no recovery code" is the claim. The idempotency key
covers the one crash window replay can't distinguish (durable but
unacknowledged): re-appending step N returns the original receipt instead
of duplicating.

### Final verification

After the child completes, the parent replays the stream and asserts, out
loud: every step 1..N present **exactly once**, monotonically ordered, batch
receipts consistent. This mirrors the storm demo's "verify every record
survived" close and is the demo's proof-of-claim, not just narration.

### Tests — `examples/py/test_unkillable_agent.py`

1. Happy path: no kills ⇒ all steps once, exit 0.
2. Deterministic kills (`SALAMANDER_KILL_AT=3,5` on `tmp_path`): parent runs
   three children; final replay has each step exactly once.
3. Idempotent retry: simulate the durable-but-unacked window by re-invoking
   a step's `append_batch` with the same key; assert same receipt, no
   duplicate event.
4. Kill at step 1 and at the final step (edge kill points).
5. Resume computation as a pure function (`done`/`next_step`) unit-tested
   directly.

Tests drive the parent loop in-process where possible; the subprocess path
gets one integration test so CI covers the real re-open-after-death cycle.

---

## Documentation and release integration

- Add both demos to the READMEs: root README "Demos" section (Python demos
  get equal billing with the Rust ones) and `salamander-py/README.md`.
- Record both for the `v0.1.0` demo-assets roadmap item; `--mock` mode makes
  the `chat.py` recording reproducible.
- The pytest suites join the documented `python -m pytest examples/py`
  flow; no new CI wiring beyond what the maturin CI item already plans.

## Acceptance criteria

1. Fresh clone + `maturin develop` + each demo command runs to completion in
   under a minute, offline, on Linux/macOS/Windows.
2. `chat.py`: fork/rewind/switch work as specced; the main timeline is
   byte-identical after any amount of branching; a cold restart shows all
   branches.
3. `unkillable_agent.py`: with two kills, three processes total, final
   replay contains every step exactly once; the demo's printed claims are
   all mechanically asserted, not just narrated.
4. Both pytest suites pass in CI without network access or API keys.
5. No state persisted outside the engine directory by either demo.

## Out of scope (v1)

- Forking from a branch in `chat.py` (binding exposes default-timeline
  forks only).
- Streaming model output, tool use, or multi-conversation management in
  `chat.py` — it is a storage demo wearing a chat UI, not a chat product.
- Torn-write injection in `unkillable_agent.py` — that is the Rust crash
  harness's job; this demo claims resume semantics, not media-level
  durability.

## Open questions — resolved

1. `/diff A B` — **yes**, implemented as a diff between two *timelines*
   (branch names, or `main`): shared-prefix length plus each side's
   divergent suffix. Between-offsets diff on one timeline is subsumed by
   `/rewind`; the cross-branch form is the one that previews the
   "structured why-queries" roadmap idea.
2. Mock-model personality — template grammar (six templates, seeded by
   conversation position *and* the last user turn, so identical histories
   reply identically while diverged branches visibly diverge).
3. `--chaos` flag — **no**; the seeded-random default schedule plus
   `SALAMANDER_KILL_SEED` already gives reproducible variety without
   diluting the one-command story.

## Implementation notes (July 2026)

Deviations and discoveries from building the spec:

- **User-facing turn numbers are transcript positions**, not engine
  offsets. `chat.py` translates a position to a log offset only at the
  fork boundary (`rows[n]["offset"]`, or `head()` for n = len). Engine
  offsets never appear in the UI.
- **The branch list is itself derived state.** The binding has no
  list-branches API, so `chat.py` records each fork as an event in a
  `_branches` registry namespace and replays it on demand — the "no state
  outside the engine" rule applied to the demo's own bookkeeping.
- **The supervisor removes the stale `LOCK` file** after a killed worker,
  exactly as the Rust crash harness parent does: the worker died before
  its lock guard could run, and the supervisor knows it is gone.
- **The demos caught a real engine bug**: paged replay livelocked when the
  records past the last yielded one were all branch-scope-filtered (e.g.
  replaying main while a fork's events sit at the log tail). Fixed in
  `facade.rs::next_page` (adopt the reader's continuation on an exhausted
  scan), defended by
  `engine_facade::paged_replay_terminates_when_filtered_records_trail_the_scan`,
  and recorded in [CHANGELOG.md](../../CHANGELOG.md).
