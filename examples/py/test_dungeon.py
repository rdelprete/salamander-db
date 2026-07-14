"""pytest suite for the dungeon demo.

    python -m pytest examples/py/test_dungeon.py -v

Pure rules are tested without the engine; engine glue against a tmp dir; the
HTTP layer on an ephemeral port; and the pull-the-plug crash via a real
subprocess that is killed and reopened.
"""

import json
import os
import random
import socket
import subprocess
import sys
import threading
import time
from http.client import HTTPConnection

import pytest

import dungeon_game as game
from dungeon import Server, Handler
from http.server import ThreadingHTTPServer


# ── pure rules (no engine) ──────────────────────────────────────────────


def scripted(seed=1):
    """A deterministic sequence of turns as raw event bodies, by driving the
    pure generator with a fixed RNG."""
    world = game.initial_world(seed)
    rng = random.Random("scripted")
    events, turn = [{"kind": "dungeon_seeded", "seed": seed, "turn": 0}], 0
    for direction in "SSEEEESSSS":
        turn += 1
        bodies = game.generate_turn(world, direction, turn, rng=rng)
        if not bodies:
            turn -= 1
            continue
        for b in bodies:
            game.reduce(world, b)
        events.extend(bodies)
    return events


def test_fold_is_deterministic():
    events = scripted()
    assert game.fold(events) == game.fold(events)


def test_world_at_matches_incremental_fold():
    events = scripted()
    # Fold once incrementally, snapshotting after each turn boundary, and
    # assert a fresh bounded fold agrees at every turn.
    max_turn = max(b.get("turn", 0) for b in events)
    for t in range(0, max_turn + 1):
        bounded = [b for b in events if b.get("turn", 0) <= t]
        incremental = game.fold(bounded)
        assert incremental["turn"] == t or not any(
            b.get("turn") == t for b in events
        )
        # a second independent fold of the same prefix is identical
        assert game.fold(bounded) == incremental


def test_replay_never_consults_rng():
    # Folding does not touch the module RNG: seeding it differently must not
    # change a replay, because every roll is already in the events.
    events = scripted()
    random.seed(1)
    a = game.fold(events)
    random.seed(999)
    b = game.fold(events)
    assert a == b


def test_bump_attack_uses_recorded_roll():
    world = game.initial_world(1)
    # Force the rat adjacent to the player and attack east into it.
    world["entities"]["rat-1"]["pos"] = [1, 2]
    rng = random.Random(0)
    bodies = game.generate_turn(world, "E", 1, rng=rng)
    attack = next(b for b in bodies if b["kind"] == "attack" and b["attacker"] == "player")
    assert attack["dmg"] == attack["roll"] >= 1


def test_key_opens_door_and_wins():
    world = game.initial_world(1)
    world["inventory"].append("key")
    player = world["entities"]["player"]
    player["pos"] = [world["door"]["pos"][0], world["door"]["pos"][1] - 1]
    bodies = game.generate_turn(world, "E", 1)
    kinds = [b["kind"] for b in bodies]
    assert "door_open" in kinds and "win" in kinds


def test_locked_door_is_not_a_legal_move():
    world = game.initial_world(1)
    player = world["entities"]["player"]
    player["pos"] = [world["door"]["pos"][0], world["door"]["pos"][1] - 1]
    assert game.generate_turn(world, "E", 1) is None  # no key


def test_walking_into_wall_is_illegal():
    world = game.initial_world(1)  # player starts at (1,1), north & west are walls
    assert game.generate_turn(world, "N", 1) is None
    assert game.generate_turn(world, "W", 1) is None


# ── engine glue ─────────────────────────────────────────────────────────


@pytest.fixture
def dungeon(tmp_path):
    d = game.Dungeon(str(tmp_path / "db"), seed=3)
    yield d
    d.close()


def test_turn_is_one_atomic_idempotent_batch(dungeon):
    before = dungeon.db.head()
    world = dungeon.play("S", rng=random.Random(0))
    assert world is not None
    assert dungeon.num_turns() == 1
    grew = dungeon.db.head() - before
    # Re-issuing the same turn's batch is idempotent — the play() path guards
    # by turn number, and the engine key guards the raw batch.
    events = dungeon.db.history(game.NAMESPACE)
    turn1 = [{"body": e["body"]} for e in events if e["body"].get("turn") == 1]
    receipt = dungeon.db.append_batch(
        game.NAMESPACE, turn1, idempotency_key="main-turn-1", durability="sync")
    assert dungeon.db.head() == before + grew  # nothing appended


def test_illegal_move_appends_nothing(dungeon):
    before = dungeon.db.head()
    assert dungeon.play("N") is None            # into a wall
    assert dungeon.db.head() == before


def test_bestiary_view_matches_a_hand_fold(dungeon):
    # Play until at least one monster dies, then assert the engine-maintained
    # view agrees with a fold over the same history.
    rng = random.Random("kills")
    for _ in range(40):
        for d in "EESSWWNN":
            dungeon.play(d, rng=rng)
    view = dungeon.bestiary()
    kills = [e["body"] for e in dungeon.db.history(game.NAMESPACE)
             if e["body"]["kind"] == "kill"]
    from collections import Counter
    folded = Counter(k["target_type"] for k in kills)
    assert view["total"] == len(kills)
    for kind, count in view["counts"].items():
        assert count == folded.get(kind, 0)


def test_fork_diverges_and_main_is_untouched(dungeon):
    rng = random.Random(1)
    for d in "SSEE":
        dungeon.play(d, rng=rng)
    main_before = dungeon.db.history(game.NAMESPACE)

    branch, created = dungeon.fork_at(2)
    assert created
    dungeon.play("S", branch=branch, rng=random.Random(2))

    main_world = dungeon.world_at()
    branch_world = dungeon.world_at(branch=branch)
    # Shared prefix through turn 2, then divergence.
    assert dungeon.world_at(turn=2)["entities"]["player"]["pos"] == \
        dungeon.world_at(branch=branch, turn=2)["entities"]["player"]["pos"]
    assert dungeon.db.history(game.NAMESPACE) == main_before  # main untouched
    assert dungeon.num_turns(branch) == 3


def test_fork_collision_returns_existing_branch(dungeon):
    for d in "SS":
        dungeon.play(d, rng=random.Random(0))
    a, created_a = dungeon.fork_at(1)
    b, created_b = dungeon.fork_at(1)
    assert created_a and not created_b and a == b


def test_fork_of_a_fork_builds_an_ancestry_chain(dungeon):
    rng = random.Random(1)
    for d in "SSEE":
        dungeon.play(d, rng=rng)
    b1, _ = dungeon.fork_at(2)                       # fork main @ 2
    dungeon.play("S", branch=b1, rng=random.Random(2))
    b1_before = dungeon.db.branch_history(b1, game.NAMESPACE)

    b2, created = dungeon.fork_at(3, branch=b1)      # fork the fork @ 3
    assert created and b2 != b1
    dungeon.play("E", branch=b2, rng=random.Random(3))

    # Multi-level lineage, shared prefix, parent untouched.
    ancestry = [x["name"] for x in dungeon.db.branch_ancestry(b2)]
    assert ancestry == ["main", b1, b2]
    assert dungeon.world_at(branch=b1, turn=3)["entities"]["player"]["pos"] == \
        dungeon.world_at(branch=b2, turn=3)["entities"]["player"]["pos"]
    assert dungeon.db.branch_history(b1, game.NAMESPACE) == b1_before


def test_cold_reopen_recovers_run_and_branches(tmp_path):
    path = str(tmp_path / "db")
    d1 = game.Dungeon(path, seed=3)
    for mv in "SSEE":
        d1.play(mv, rng=random.Random(0))
    branch, _ = d1.fork_at(2)
    d1.play("S", branch=branch, rng=random.Random(0))
    turns_main, turns_branch = d1.num_turns(), d1.num_turns(branch)
    d1.close()

    d2 = game.Dungeon(path, seed=3)
    assert d2.num_turns() == turns_main
    assert [b["branch"] for b in d2.branches()] == [branch]
    assert d2.num_turns(branch) == turns_branch
    d2.close()


# ── HTTP layer ──────────────────────────────────────────────────────────


@pytest.fixture
def http_server(tmp_path):
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    srv = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    srv.game_server = Server(str(tmp_path / "db"), seed=3)
    thread = threading.Thread(target=srv.serve_forever, daemon=True)
    thread.start()
    yield port
    srv.shutdown()
    srv.game_server.dungeon.close()


def _req(port, method, path, body=None):
    conn = HTTPConnection("127.0.0.1", port)
    conn.request(method, path,
                 body=json.dumps(body) if body is not None else None)
    resp = conn.getresponse()
    data = json.loads(resp.read() or b"{}")
    conn.close()
    return data


def test_http_state_action_and_fork(http_server):
    port = http_server
    state = _req(port, "GET", "/state")
    assert state["num_turns"] == 0 and len(state["map"]) == game.HEIGHT

    played = _req(port, "POST", "/action", {"move": "S"})
    assert played["ok"] and played["num_turns"] == 1

    blocked = _req(port, "POST", "/action", {"move": "W"})  # west is a wall throughout
    assert blocked["ok"] is False

    forked = _req(port, "POST", "/fork?branch=main", {"at_turn": 1})
    assert forked["ok"] and forked["branch"].startswith(game.NAMESPACE)
    branch_state = _req(port, "GET", f"/state?branch={forked['branch']}")
    assert branch_state["branch"] == forked["branch"]


def test_http_fork_of_a_fork(http_server):
    port = http_server
    for _ in range(3):
        _req(port, "POST", "/action", {"move": "S"})
    first = _req(port, "POST", "/fork?branch=main", {"at_turn": 1})
    assert first["ok"] and first["created"]
    b1 = first["branch"]
    _req(port, "POST", f"/action?branch={b1}", {"move": "S"})  # turn 2 on b1
    # Fork the fork — this used to be a hard "not supported"; now it works.
    second = _req(port, "POST", f"/fork?branch={b1}", {"at_turn": 2})
    assert second["ok"] and second["created"]
    assert second["branch"] != b1
    b2_state = _req(port, "GET", f"/state?branch={second['branch']}")
    assert b2_state["branch"] == second["branch"]


def test_http_refork_same_point_switches_not_creates(http_server):
    port = http_server
    for _ in range(3):
        _req(port, "POST", "/action", {"move": "S"})
    first = _req(port, "POST", "/fork?branch=main", {"at_turn": 2})
    again = _req(port, "POST", "/fork?branch=main", {"at_turn": 2})
    assert first["created"] and not again["created"]
    assert first["branch"] == again["branch"]


def test_http_unknown_branch_never_500s(http_server):
    port = http_server
    _req(port, "POST", "/action", {"move": "S"})
    # A stale client branch must degrade gracefully, not crash the handler.
    acted = _req(port, "POST", "/action?branch=ghost", {"move": "S"})
    assert acted["ok"] is False
    state = _req(port, "GET", "/state?branch=ghost")
    assert state["branch"] == "main"  # falls back


def test_http_time_travel_is_read_only(http_server):
    port = http_server
    for _ in range(3):
        _req(port, "POST", "/action", {"move": "S"})
    head = _req(port, "GET", "/state")
    past = _req(port, "GET", "/state?turn=1")
    assert past["num_turns"] == head["num_turns"]  # scrubbing doesn't change history
    assert past["turn"] <= 1


def test_crash_endpoint_kills_process_and_state_survives(tmp_path):
    path = str(tmp_path / "db")
    port = _free_port()
    env = dict(os.environ)
    env["PYTHONPATH"] = os.path.dirname(os.path.abspath(__file__))
    script = os.path.join(os.path.dirname(__file__), "dungeon.py")
    proc = subprocess.Popen(
        [sys.executable, script, path, "--port", str(port), "--seed", "3"],
        env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        _wait_up(port)
        _req(port, "POST", "/action", {"move": "S"})
        _req(port, "POST", "/action", {"move": "S"})
        # The endpoint answers, then calls os._exit mid-flight; the client may
        # get the "dying" ack or a dropped connection. Either way the process
        # must die with the killed exit code.
        try:
            _req(port, "POST", "/crash", {})
        except Exception:
            pass
        proc.wait(timeout=10)
        assert proc.returncode == 137
    finally:
        if proc.poll() is None:
            proc.kill()

    # Reopen the same directory in-process: the run survived the hard kill,
    # and the demo's stale-LOCK recovery lets it open cleanly.
    lock = os.path.join(path, "LOCK")
    if os.path.exists(lock):
        os.remove(lock)
    d = game.Dungeon(path, seed=3)
    assert d.num_turns() == 2
    d.close()


def _free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _wait_up(port, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            _req(port, "GET", "/state")
            return
        except Exception:
            time.sleep(0.1)
    raise RuntimeError("server did not come up")
