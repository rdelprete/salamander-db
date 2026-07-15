# Spec: `dungeon.py` — **The Undying Dungeon**, a browser game over the log

**Status:** implemented (see Implementation notes at the end)
**Target:** post-showcase demo asset; Python cousin of the
Rust playground (`salamander-demo -- ui`) and a seed for the
`salamander-scope` inspector ([ROADMAP.md](../../ROADMAP.md))
**Location:** [`examples/py/`](../../examples/py) — `dungeon.py` (game rules,
engine glue, stdlib HTTP server) + `dungeon.html` (one page, inline CSS/JS,
no frameworks) + `test_dungeon.py`.

## Motivation

`chat.py` and `unkillable_agent.py` demo the engine to people who read
terminals. A tiny game demos it to everyone else: the same primitives —
crash-proof state, time travel, forking timelines — presented as *game
mechanics* people already understand (rewind, ghosts, "what if I'd taken
the other door"). The design goal is that every UI control is a thin label
over an engine operation, and the player can *see* that.

The pitch line: **a roguelike where dying is a query, rewinding is a
replay, and the save file cannot corrupt — because there is no save file.**

## Technology decision

Browser UI served by **`http.server` from the stdlib**, one embedded page,
JSON over `fetch`. Rejected: pygame/pyxel/textual (a required install for a
demo violates the zero-dependency rule the other demos set, and a native
window can't be clicked from a README link or recorded as easily). This
mirrors the Rust playground's "no dependencies, no server framework — one
command" stance:

```
python examples/py/dungeon.py            # serves http://127.0.0.1:7172
```

The server process owns the single `salamander.open` handle (it *is* the
single writer); the browser is a stateless view. Data dir defaults to
`./dungeon-mem` and persists — quitting and relaunching resumes the run.

## Game design (deliberately tiny)

- **Fixed handcrafted dungeon**, one screen, ~12×9 tiles: walls, floor, the
  player 🧙, a few monsters (🐀 🦇 and one boss 🦎 — a salamander, of
  course), items (🗝️ key, 🧪 potion, 💰 gold), a locked exit 🚪.
- **Turn-based, bump-to-act**: arrows/WASD move; moving into a monster
  attacks it; monsters take their step in the same turn. No physics, no
  animation state — a turn is a pure function of (world, action, recorded
  rolls).
- **Win**: take the key, open the door. **Death**: HP hits 0 — and the
  *only* continue option is the engine pitch: scrub back, fork, try a
  different line. Death is not game over; it's a branch point.
- 5–10 minutes to win. Content is not the point.

## Event model

One namespace, `"run"`, one run per data directory (a fresh dungeon =
point the CLI at a fresh dir). Turn 0 is `{"kind": "dungeon_seeded",
"seed": …, "layout_version": 1}` — the map derives deterministically from
the seed, so the world is reconstructable from the log alone.

**One turn = one atomic batch** (`append_batch`, `idempotency_key="turn-N"`,
`durability="sync"`): the player's action plus every consequence —
monster moves, damage both ways, pickups, death — commits all-or-nothing.
A crash can never leave "the rat bit you but you never swung":

```python
db.append_batch("run", [
    {"body": {"kind": "player_move", "to": [4, 3]}},
    {"body": {"kind": "attack", "attacker": "rat-2", "target": "player",
              "roll": 14, "dmg": 2}},   # roll recorded, never re-rolled
], idempotency_key=f"turn-{n}", durability="sync")
```

**Determinism rule (normative):** every random outcome is stored *in* the
event (`roll`, `dmg`, loot drops). Replay never consults an RNG; a
replayed world is byte-identical to the one the player lived, at every
offset, on every branch. A test defends this by folding twice and
comparing.

The world state is one pure fold, `fold(events) -> World` (entities, HP,
positions, inventory, door state); `world_at(n)` is the same fold over
`replay("run", end=n)`. Branch listing reuses `chat.py`'s registry
pattern: each fork appends `{"branch", "at_offset", "at_turn"}` to a
`_branches` namespace — no state outside the engine, including the demo's
own bookkeeping.

## Server API (JSON; all state derived per request, nothing cached)

| Endpoint | Engine operation |
|---|---|
| `GET /` | serve `dungeon.html` |
| `GET /state?branch=B&turn=N` | fold of `replay`/`branch_history` bounded at N (omit N = head); includes world, turn count, event-log tail, and ghost data (below) |
| `POST /action {"move": "N|S|E|W"}` | build + append the turn batch on the current branch |
| `POST /fork {"at_turn": N}` | `fork("run", offset)` + registry append; returns branch name |
| `GET /branches` | registry replay |
| `POST /crash` | **`os._exit(137)`** — no goodbye. The UI's scary red button. |

The crash endpoint is the whole durability pitch in one click: the page
goes dead mid-dungeon, the player relaunches `dungeon.py`, and the run is
exactly there — including timelines. (Like the unkillable agent, the
restart mirrors the Rust crash harness's stale-`LOCK` cleanup: remove the
`LOCK` file on open only if no server process is alive; v1 keeps this a
printed instruction rather than automatic, since here no supervisor knows
the worker is dead.)

## UI (one page, inline everything, emoji tiles — zero art assets)

- **Board**: CSS grid of emoji tiles. No canvas, no sprite sheets.
- **Side panel**: HP hearts, gold, inventory — and the **live event log
  tail**, showing the actual JSON bodies as they append. The debug view
  *is* the data model; that's the message.
- **Timeline scrubber** (the star): a slider 0..head. Dragging renders
  `world_at(n)` — the monster un-dies, the potion un-drinks. A ▶ button
  replays turn-by-turn (death replays for free). While scrubbed back, the
  board is read-only and a **⑂ Branch here** button appears.
- **Branch bar**: tabs for main + forks; switching re-renders from that
  timeline's replay. **Ghost toggle**: render the sibling timeline's
  player position as a translucent 👻 on the same board — two futures over
  one shared past, visible at a glance.
- **💀 Pull the plug** button → `POST /crash`, styled like it means it,
  with a tooltip: "kills the process mid-write; relaunch and nothing is
  lost."
- Keyboard: WASD/arrows for movement, `[`/`]` to scrub.

## Tests — `examples/py/test_dungeon.py` (offline, deterministic)

1. **Fold determinism**: same event list folded twice → identical worlds;
   `world_at(n)` for every n along a scripted game never disagrees with an
   incrementally-maintained world.
2. **Turn atomicity**: a scripted turn's batch is all-or-nothing; an
   idempotent re-append of `turn-N` returns the original receipt and adds
   nothing.
3. **Game rules**: bump-attack resolves with the recorded roll; key opens
   door; death flags the world dead (pure-function tests, no engine).
4. **Fork + ghost**: fork before the fatal turn, diverge, assert shared
   prefix, divergent suffixes, and main untouched (same shape as chat.py's
   tests).
5. **HTTP integration**: start the server on an ephemeral port in-process,
   drive a short game via `http.client`, scrub, fork, and assert JSON
   responses; one subprocess test exercises `POST /crash` → restart →
   state intact.

CI: add `test_dungeon.py` to the workflow's explicit pytest list.

## Acceptance criteria

1. `python examples/py/dungeon.py` on a fresh clone (extension built) opens
   a playable dungeon; a full win takes under 10 minutes; no dependency
   beyond the stdlib and the extension.
2. Pull-the-plug mid-fight, relaunch: the run resumes exactly, timelines
   included, with no recovery code beyond reopening the directory.
3. Scrubbing to any turn renders a world identical to what was on screen
   at that turn (defended by the determinism test, demonstrated by the ▶
   replay).
4. Fork-after-death is playable end-to-end: die, scrub, branch, win on the
   branch, and `/branches` + ghost view show both timelines over the
   shared prefix.
5. All tests pass offline in CI; no state outside the data directory.

## Out of scope (v1)

- Multiplayer, real-time anything, sound, animation beyond CSS.
- Procedural depth, multiple floors, balance. One handcrafted room.
- Multiple runs per directory, run management UI.
- Automatic stale-`LOCK` detection (printed instruction in v1).

## Open questions — resolved

1. Ghost rendering — **one selected ghost**: a dropdown picks which other
   timeline to overlay as a translucent 👻. Readable beats complete.
2. Download-log-as-JSON — **deferred**; left for the real
   `salamander-scope` inspector rather than bolted onto the game.
3. Port — **fixed 7172** (the Rust playground is 7171), README-friendly.

## Implementation notes (July 2026)

- **Three files, not two.** Rules + engine glue live in
  `dungeon_game.py` (importable, engine-testable with no HTTP); the server
  and CLI are `dungeon.py`; the page is `dungeon.html`. The split is what
  lets the pure-rules and view-matches-fold tests run without a socket.
- **The bestiary view is default-timeline-scoped.** Registered views fold
  across namespaces but are pinned to branch `[0;16]` at head (verified in
  `facade.rs`), so the "Bestiary · engine index" panel is honestly labeled
  *main line* and the game state stays a fold (only a fold answers
  `view_at`). A test asserts the view equals a hand fold of the same
  history.
- **Turn numbers are transcript positions**; the scrubber maps a turn to a
  log offset and calls `replay(end=…)` on the default timeline — the engine
  time-travel primitive — slicing branch history for forks (same
  translation `chat.py` uses).
- **Stale-LOCK recovery is safe because the HTTP port is the interlock.**
  The server binds 7172 *before* opening the DB, so a free port proves no
  sibling server is alive and any `LOCK` left by pull-the-plug is stale and
  clearable — the same reasoning the crash-harness supervisor uses. This
  upgrades the spec's "printed instruction" to automatic recovery.
- **UI robustness:** movement ignores held-key auto-repeat (`e.repeat`) and
  guards against overlapping async turns, so a leaned-on arrow key can't
  race a burst of turns into the log.
- **No handler ever 500s.** Every request routes through a boundary
  try/except that returns a JSON error, because a dropped connection makes
  the browser's `fetch` reject and the UI silently do nothing. Re-forking
  the same point notifies "already branched → switched", and a stale client
  branch falls back to main. Regression-tested in `test_dungeon.py`.
- **Fork of a fork.** Originally scoped out because the Python binding
  hardcoded the default timeline as the fork parent — but the *engine*
  always supported forking any branch (`db.fork_branch(parent, at, …)`,
  multi-level ancestry). The binding's `fork()` now takes an optional
  `parent` branch name, so the game forks the current timeline at any depth:
  `main → run-fork-9 → run-fork-21`, with `branch_ancestry` returning the
  full chain. The fork button is enabled on any branch (just scrub back
  first); tested at both the engine-glue and HTTP layers.
- Verified end-to-end in a real browser: movement, the time-travel
  scrubber (turns 0/1/2 render distinct rewound worlds), fork-while-scrubbed
  (main untouched, both tabs), the ghost overlay, and a `POST /crash` →
  relaunch → run-intact cycle. Twenty offline tests
  (`test_dungeon.py`) cover the same ground and are wired into CI.
- **README video is generated, not hand-recorded.** `examples/py/record_demo.py`
  starts its own server, drives the money-loop with Playwright (fight →
  rewind → fork → diverge → pull-the-plug → reload-intact), records it in
  real time, and transcodes to `docs/assets/dungeon-demo.mp4` (~0.5 MiB,
  H.264, 1000×720, ~21 s). Deterministic (fixed seed + fixed script), so the
  asset is reproducible — re-run when the UI changes. Playwright and
  `imageio-ffmpeg` are dev-only tooling for this script, not runtime or test
  dependencies of the demo.
