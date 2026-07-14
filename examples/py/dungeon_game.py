"""The Undying Dungeon — rules and engine glue, with no HTTP in sight.

The design rule that makes every mechanic work: **the world is never
stored, only derived.** State is a pure fold over a log of events, so
``world_at(n)`` is that same fold stopped early, a fork is a second future
over a shared past, and a crash loses nothing because there is no save file
to corrupt — only the append-only log.

Two invariants keep replay honest (each is defended by a test):

- Every random outcome (attack rolls, damage) is written *into* the event.
  Replay never rolls dice; a replayed world is identical to the one the
  player lived, at every offset, on every branch.
- One turn is one atomic batch. The player's move and all its consequences
  (monsters stepping, damage both ways, a death) commit all-or-nothing, so
  "the rat bit you but you never swung" cannot exist, even across a crash.

The bestiary panel is the one piece of state we *don't* fold by hand: it is
a registered view — a secondary index the engine maintains incrementally on
the main timeline — so "how many bats have I felled" is answered without
replaying anything. See ``Dungeon.bestiary``.
"""

import copy
import random

import salamander

NAMESPACE = "run"
REGISTRY_NS = "_branches"  # fork metadata — itself just events in the log
BESTIARY_VIEW = "bestiary"

# One handcrafted room, 12x9. Legend: '#' wall, '.' floor, '@' player start,
# 'D' locked exit; monsters r/b/S; items k(ey) p(otion) $(gold).
MAP = [
    "############",
    "#@..r......#",
    "#.....##...#",
    "#..#..k....#",
    "#..#..##...D",
    "#.b#.......#",
    "#..#..##...#",
    "#p...$...S.#",
    "############",
]

HEIGHT = len(MAP)
WIDTH = len(MAP[0])

MONSTER_TYPES = {
    "r": ("rat", 4, (1, 2)),        # glyph: (type, hp, damage range)
    "b": ("bat", 3, (1, 3)),
    "S": ("salamander", 12, (3, 6)),
}
ITEM_TYPES = {"k": "key", "p": "potion", "$": "gold"}

PLAYER_HP = 20
POTION_HEAL = 5

DIRS = {"N": (-1, 0), "S": (1, 0), "E": (0, 1), "W": (0, -1)}


# ── the world is a fold over events ─────────────────────────────────────


def initial_world(seed):
    """The dungeon as the seed alone describes it — turn 0 of any replay."""
    entities, items, door, counts = {}, {}, None, {}
    entities["player"] = {
        "type": "player", "pos": None, "hp": PLAYER_HP,
        "max_hp": PLAYER_HP, "alive": True,
    }
    for r, row in enumerate(MAP):
        assert len(row) == WIDTH, f"map row {r} is not {WIDTH} wide"
        for c, ch in enumerate(row):
            if ch == "@":
                entities["player"]["pos"] = [r, c]
            elif ch in MONSTER_TYPES:
                kind, hp, _ = MONSTER_TYPES[ch]
                counts[kind] = counts.get(kind, 0) + 1
                mid = f"{kind}-{counts[kind]}"
                entities[mid] = {
                    "type": kind, "pos": [r, c], "hp": hp,
                    "max_hp": hp, "alive": True,
                }
            elif ch in ITEM_TYPES:
                name = ITEM_TYPES[ch]
                items[name] = {"pos": [r, c], "taken": False}
            elif ch == "D":
                door = {"pos": [r, c], "open": False}
    return {
        "seed": seed,
        "turn": 0,
        "entities": entities,
        "items": items,
        "door": door,
        "inventory": [],
        "status": "playing",
    }


def reduce(world, body):
    """Apply one event body to the world in place. The single source of
    truth: both replay and live turn generation go through here, so a
    generated turn can never disagree with its own replay."""
    kind = body["kind"]
    if kind == "dungeon_seeded":
        return  # the initial world already encodes the seed
    world["turn"] = max(world["turn"], body.get("turn", 0))
    if kind == "player_move":
        world["entities"]["player"]["pos"] = list(body["to"])
    elif kind == "monster_move":
        world["entities"][body["entity"]]["pos"] = list(body["to"])
    elif kind == "attack":
        world["entities"][body["target"]]["hp"] -= body["dmg"]
    elif kind == "kill":
        world["entities"][body["target"]]["alive"] = False
    elif kind == "pickup":
        item = body["item"]
        world["inventory"].append(item)
        world["items"][item]["taken"] = True
        if item == "potion":
            player = world["entities"]["player"]
            player["hp"] = min(player["max_hp"], player["hp"] + POTION_HEAL)
    elif kind == "door_open":
        world["door"]["open"] = True
    elif kind == "player_death":
        world["entities"]["player"]["alive"] = False
        world["status"] = "dead"
    elif kind == "win":
        world["status"] = "won"
    else:
        raise ValueError(f"unknown event kind {kind!r}")


def fold(events):
    """Rebuild the world from a sequence of event rows (each ``{"body": …}``
    or a bare body)."""
    seed = 0
    bodies = [ev["body"] if "body" in ev else ev for ev in events]
    if bodies:
        seed = bodies[0].get("seed", 0)
    world = initial_world(seed)
    for body in bodies:
        reduce(world, body)
    return world


# ── geometry + rules helpers (pure) ─────────────────────────────────────


def _add(pos, delta):
    return [pos[0] + delta[0], pos[1] + delta[1]]


def is_wall(pos):
    r, c = pos
    if not (0 <= r < HEIGHT and 0 <= c < WIDTH):
        return True
    return MAP[r][c] == "#"


def is_door(world, pos):
    return world["door"] is not None and list(pos) == world["door"]["pos"]


def occupant(world, pos):
    """Id of the living entity standing on ``pos``, or None."""
    for eid, e in world["entities"].items():
        if e["alive"] and e["pos"] == list(pos):
            return eid
    return None


def item_at(world, pos):
    for name, item in world["items"].items():
        if not item["taken"] and item["pos"] == list(pos):
            return name
    return None


def _step_toward(world, src, dst):
    """One floor step from ``src`` that reduces distance to ``dst``, or None
    if both preferred tiles are blocked. Deterministic — no RNG."""
    dr, dc = dst[0] - src[0], dst[1] - src[1]
    axes = [(0 if dr == 0 else (1 if dr > 0 else -1), 0),
            (0, 0 if dc == 0 else (1 if dc > 0 else -1))]
    # Try the larger delta first, so monsters home in naturally.
    if abs(dc) > abs(dr):
        axes.reverse()
    for delta in axes:
        if delta == (0, 0):
            continue
        target = _add(src, delta)
        if not is_wall(target) and not is_door(world, target) \
                and occupant(world, target) is None:
            return target
    return None


def monster_ids(world):
    """Living monsters in a stable order, so a turn is reproducible."""
    return sorted(
        eid for eid, e in world["entities"].items()
        if eid != "player" and e["alive"]
    )


def generate_turn(world, direction, turn_no, rng=None):
    """Produce the event bodies for one turn, or None if the move is illegal
    (into a wall, or into the locked door without the key).

    All consequences — the player's action, then every monster's — are
    simulated through ``reduce`` on a working copy, so what we emit and what
    a replay reconstructs are identical by construction. Rolls come from
    ``rng`` but are baked into the events; replay never needs ``rng``.
    """
    if world["status"] != "playing":
        return None
    if rng is None:
        rng = random.Random(f"{world['seed']}:{turn_no}")
    if direction not in DIRS:
        return None

    work = copy.deepcopy(world)
    emitted = []

    def do(body):
        body["turn"] = turn_no
        emitted.append(body)
        reduce(work, body)

    player = work["entities"]["player"]
    target = _add(player["pos"], DIRS[direction])

    if is_door(work, target):
        if "key" not in work["inventory"]:
            return None  # locked — not a legal move
        do({"kind": "door_open"})
        do({"kind": "player_move", "to": target})
        do({"kind": "win"})
        return emitted
    if is_wall(target):
        return None

    foe = occupant(work, target)
    if foe and foe != "player":
        _player_attack(work, foe, rng, do)
    else:
        do({"kind": "player_move", "to": target})
        item = item_at(work, target)
        if item:
            do({"kind": "pickup", "item": item})

    _monsters_act(work, rng, do)
    return emitted


def _player_attack(work, foe, rng, do):
    roll = rng.randint(1, 6)
    do({"kind": "attack", "attacker": "player", "target": foe,
        "roll": roll, "dmg": roll})
    if work["entities"][foe]["hp"] <= 0:
        do({"kind": "kill", "target": foe,
            "target_type": work["entities"][foe]["type"]})


def _monsters_act(work, rng, do):
    player = work["entities"]["player"]
    for mid in monster_ids(work):
        if not player["alive"]:
            break
        m = work["entities"][mid]
        dist = abs(m["pos"][0] - player["pos"][0]) + abs(m["pos"][1] - player["pos"][1])
        if dist == 1:
            lo, hi = MONSTER_TYPES_BY_NAME[m["type"]]
            roll = rng.randint(lo, hi)
            do({"kind": "attack", "attacker": mid, "target": "player",
                "roll": roll, "dmg": roll})
            if work["entities"]["player"]["hp"] <= 0:
                do({"kind": "player_death"})
        else:
            nxt = _step_toward(work, m["pos"], player["pos"])
            if nxt is not None:
                do({"kind": "monster_move", "entity": mid, "to": nxt})


# Damage ranges keyed by type name, for the monster-turn lookup above.
MONSTER_TYPES_BY_NAME = {
    name: dmg for (name, _hp, dmg) in MONSTER_TYPES.values()
}


# ── engine glue: turns, forks, and the no-refold bestiary ───────────────


class Dungeon:
    """Owns the single-writer handle. In-memory state is only the seed and
    the registered bestiary view; every world is a replay."""

    def __init__(self, path, seed=1):
        self.db = salamander.open(path, commit_every_count=1)
        self._ensure_seeded(seed)
        self.seed = self._seed_from_log()
        # The bestiary is a secondary index the engine maintains for us: kill
        # events grouped by monster type. Answering "how many bats?" then
        # costs a lookup, not a replay. Idempotent to re-register on reopen.
        self.db.register_view(
            BESTIARY_VIEW, key="target",
            indexes={"by_type": "target_type"},
            where_field="kind", where_value="kill",
        )

    # -- seeding -------------------------------------------------------

    def _ensure_seeded(self, seed):
        if self.db.head() == 0:
            self.db.append(NAMESPACE,
                           {"kind": "dungeon_seeded", "seed": seed, "turn": 0})
            self.db.commit()

    def _seed_from_log(self):
        first = self.db.replay(NAMESPACE, start=0, end=1)
        return first[0]["body"].get("seed", 1) if first else 1

    # -- reading (all state derived, nothing cached) ------------------

    def _events(self, branch):
        if branch is None:
            return self.db.history(NAMESPACE)
        return self.db.branch_history(branch, NAMESPACE)

    def num_turns(self, branch=None):
        events = self._events(branch)
        return max((ev["body"].get("turn", 0) for ev in events), default=0)

    def _offset_after_turn(self, events, turn):
        """The global log offset just past the last event of ``turn`` — the
        fork/replay cut point. Robust to registry events shifting offsets."""
        after = [ev["offset"] for ev in events
                 if ev["body"].get("turn", 0) > turn]
        if after:
            return after[0]
        return (events[-1]["offset"] + 1) if events else 0

    def world_at(self, branch=None, turn=None):
        """The world as of ``turn`` on ``branch`` (default: the latest turn
        on the default timeline) — a bounded replay, nothing destroyed."""
        events = self._events(branch)
        if turn is None:
            bounded = events
        else:
            end = self._offset_after_turn(events, turn)
            if branch is None:
                # The engine's time-travel primitive, bounded by real offset.
                bounded = self.db.replay(NAMESPACE, start=0, end=end)
            else:
                bounded = [ev for ev in events if ev["offset"] < end]
        world = fold(bounded)
        world["num_turns"] = self.num_turns(branch)
        world["log"] = [ev["body"] for ev in bounded][-8:]
        return world

    # -- writing: one turn = one atomic batch --------------------------

    def play(self, direction, branch=None, rng=None):
        """Append one turn to ``branch``. Returns the world after the turn,
        or None if the move was illegal (nothing is appended)."""
        world = self.world_at(branch)
        if world["status"] != "playing":
            return None
        turn_no = world["num_turns"] + 1
        bodies = generate_turn(world, direction, turn_no, rng=rng)
        if not bodies:
            return None
        self.db.append_batch(
            NAMESPACE,
            [{"body": b} for b in bodies],
            idempotency_key=f"{branch or 'main'}-turn-{turn_no}",
            durability="sync",
            **({"branch": branch} if branch else {}),
        )
        return self.world_at(branch)

    # -- forking: a second future over a shared past -------------------

    def fork_at(self, turn, branch=None):
        """Fork the timeline (``branch`` or the default) just after ``turn`` —
        a fork of a fork works the same as a fork of main. Returns
        (branch_name, created). Same-point collisions switch to the existing
        branch instead of raising, teaching the engine's semantics."""
        events = self._events(branch)
        at = self._offset_after_turn(events, turn)
        # The branch name is keyed on the global offset, so a fork at the same
        # position (e.g. a shared-prefix point) is the same branch — return it.
        for entry in self.branches():
            if entry["at_offset"] == at:
                return entry["branch"], False
        name = self.db.fork(NAMESPACE, at, parent=branch)
        self.db.append(REGISTRY_NS, {"branch": name, "at_offset": at,
                                     "at_turn": turn, "parent": branch})
        self.db.commit()
        return name, True

    def branches(self):
        """Every fork ever made — replayed from the registry, so a cold
        restart sees them all."""
        return [ev["body"] for ev in self.db.replay(REGISTRY_NS)]

    # -- the no-refold query -------------------------------------------

    def bestiary(self):
        """Kills grouped by monster type, answered from the engine-maintained
        view — no replay. Reflects the **default timeline** (that is where the
        view is scoped), i.e. the canonical campaign."""
        view = self.db.view(BESTIARY_VIEW)
        counts = {name: len(view.by("by_type", name))
                  for (_g, (name, _h, _d)) in MONSTER_TYPES.items()}
        return {"counts": counts, "total": view.len()}

    def close(self):
        self.db.commit()
        self.db.close()
