"""The Undying Dungeon — a browser roguelike where every mechanic is a
SalamanderDB primitive.

    python examples/py/dungeon.py            # http://127.0.0.1:7172

Move with the arrow keys or WASD. Bump a monster to fight it, grab the key,
open the door. When you die — and you will — you don't reload a save: you
drag the timeline back and *fork* a new future from before the fatal step.
The original run is untouched; both timelines share their history up to the
branch point.

The scary red button pulls the plug: it calls ``os._exit`` on the server
mid-play, no cleanup. Relaunch this script and reload the page — the run is
exactly where you left it, timelines and all, because the append-only log
is the only durable structure and it cannot be left half-written.

What this demonstrates, mapped to what you click:

    board + HP + inventory   a pure fold over the event log
    timeline scrubber        world_at(n) — the same fold, stopped early
    ⑂ Branch here            fork(): a second future over a shared past
    Bestiary panel           a registered view — an engine-maintained index,
                             queried without replaying anything
    💀 Pull the plug         crash-proof state: no save file to corrupt

This is a storage demo wearing a dungeon. No dependencies beyond the
extension and the standard library; the server owns the single writer, the
browser is a stateless view.
"""

import argparse
import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import salamander

import dungeon_game as game

HTML_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dungeon.html")


def open_dungeon(path, seed):
    """Open the dungeon, recovering from a stale ``LOCK`` left by the
    pull-the-plug crash. Only ever called once we already hold the HTTP port,
    so a free port proves no sibling server is alive and the lock is stale —
    the same reasoning the crash-harness supervisor uses when it clears LOCK
    after a killed worker."""
    try:
        return game.Dungeon(path, seed=seed)
    except salamander.LockedError:
        lock = os.path.join(path, "LOCK")
        if os.path.exists(lock):
            os.remove(lock)
            print("  (cleared a stale LOCK from a previous crash)")
        return game.Dungeon(path, seed=seed)


class Server:
    """Holds the one writer handle behind a lock, since the threading HTTP
    server may dispatch requests concurrently."""

    def __init__(self, path, seed):
        self.dungeon = open_dungeon(path, seed)
        self.lock = threading.Lock()

    def state(self, branch, turn):
        with self.lock:
            try:
                world = self.dungeon.world_at(branch=branch, turn=turn)
            except salamander.NotFoundError:
                branch = None  # stale client branch — fall back to main
                world = self.dungeon.world_at(branch=None, turn=turn)
            world["map"] = game.MAP
            world["branch"] = branch or "main"
            world["branches"] = [b["branch"] for b in self.dungeon.branches()]
            world["bestiary"] = self.dungeon.bestiary()
            return world

    def action(self, branch, direction):
        with self.lock:
            try:
                result = self.dungeon.play(direction, branch=branch)
            except salamander.NotFoundError:
                return {"ok": False, "reason": "unknown branch — reload the page"}
        if result is None:
            return {"ok": False, "reason": "blocked"}
        return {"ok": True, **self.state(branch, None)}

    def fork(self, branch, at_turn):
        with self.lock:
            name, created = self.dungeon.fork_at(at_turn, branch=branch)
        return {"ok": True, "branch": name, "created": created}


class Handler(BaseHTTPRequestHandler):
    server_version = "UndyingDungeon/1.0"

    def _send(self, code, body, content_type="application/json"):
        payload = body if isinstance(body, bytes) else json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _query(self):
        from urllib.parse import parse_qs, urlparse
        params = parse_qs(urlparse(self.path).query)
        branch = params.get("branch", ["main"])[0]
        branch = None if branch == "main" else branch
        turn = params.get("turn", [None])[0]
        return branch, (int(turn) if turn is not None else None)

    def _body(self):
        length = int(self.headers.get("Content-Length", 0))
        return json.loads(self.rfile.read(length) or b"{}")

    def do_GET(self):
        self._dispatch(self._route_get)

    def do_POST(self):
        self._dispatch(self._route_post)

    def _dispatch(self, route_fn):
        # Safety net: any handler error becomes a JSON body, never a dropped
        # connection. A 500 here would make the browser's fetch reject and the
        # UI silently do nothing — the opposite of a good demo.
        try:
            route_fn()
        except Exception as exc:  # noqa: BLE001 — deliberately catch-all at the boundary
            self._send(200, {"ok": False, "error": f"{type(exc).__name__}: {exc}"})

    def _route_get(self):
        game_server = self.server.game_server
        route = self.path.split("?", 1)[0]
        if route == "/":
            with open(HTML_PATH, "rb") as fh:
                self._send(200, fh.read(), "text/html; charset=utf-8")
        elif route == "/state":
            branch, turn = self._query()
            self._send(200, game_server.state(branch, turn))
        else:
            self._send(404, {"error": "not found"})

    def _route_post(self):
        game_server = self.server.game_server
        route = self.path.split("?", 1)[0]
        if route == "/action":
            branch, _ = self._query()
            direction = self._body().get("move", "").upper()
            self._send(200, game_server.action(branch, direction))
        elif route == "/fork":
            branch, _ = self._query()
            at_turn = int(self._body().get("at_turn", 0))
            self._send(200, game_server.fork(branch, at_turn))
        elif route == "/crash":
            # The whole durability pitch in one endpoint: die mid-write with
            # no cleanup, no flush, no goodbye. Whatever was committed stays;
            # relaunching replays it. (A real supervisor would clear the stale
            # LOCK; here the player just reruns the script.)
            self._send(200, {"ok": True, "dying": True})
            self.wfile.flush()
            os._exit(137)
        else:
            self._send(404, {"error": "not found"})

    def log_message(self, *args):
        pass  # keep the console clean for the demo


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("data_dir", nargs="?", default="./dungeon-mem",
                        help="database directory (persists across runs)")
    parser.add_argument("--port", type=int, default=7172)
    parser.add_argument("--seed", type=int, default=1,
                        help="dungeon seed (only used when the dir is fresh)")
    args = parser.parse_args(argv)

    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    server.game_server = Server(args.data_dir, args.seed)
    url = f"http://127.0.0.1:{args.port}"
    print(f"The Undying Dungeon — {url}")
    print(f"  data dir: {args.data_dir} (persists; delete it to start fresh)")
    print("  move: arrows / WASD   scrub: [ ]   Ctrl-C to stop")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nclosing — the dungeon is safe on disk")
    finally:
        server.game_server.dungeon.close()


if __name__ == "__main__":
    main()
