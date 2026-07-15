"""A chat CLI with /rewind and /fork — the "edit an earlier message and
regenerate" feature every chat product has, in one file, because the
storage engine natively does it.

Every turn is an event; the transcript is a replay; the model's context
window is a projection of the log. Rewind is a bounded replay (nothing is
destroyed), fork is an engine branch (the original timeline is untouched),
and the branch list itself is derived from events in a registry namespace —
no state lives outside the database directory.

Run (after building the extension with `maturin develop`):

    python examples/py/chat.py            # ./chat-mem, persists across runs

Talks to the Claude API when the `anthropic` package is installed and
ANTHROPIC_API_KEY is set; otherwise (or with --mock) a deterministic local
mock replies, so the demo needs no network. Commands: /help /history
/rewind /fork /branches /switch /diff /quit.
"""

import argparse
import os
import random
import sys

import salamander

REGISTRY_NS = "_branches"  # fork metadata, itself just events in the log
MAIN = "main"              # display name for the default timeline
CONTEXT_TURNS = 40         # how much transcript the model sees


class ChatError(Exception):
    """A user mistake (bad turn number, unknown branch) — printed, not fatal."""


def _drain(reader):
    """Every row from a paginated engine reader (e.g. a diff suffix)."""
    rows = []
    while True:
        page = reader.next_page()
        rows.extend(page["records"])
        if page["done"]:
            return rows


# ── model backends ──────────────────────────────────────────────────────


class MockModel:
    """Deterministic, seeded, offline — recordings and CI use this."""

    name = "mock"

    _TEMPLATES = [
        "{topic}? Bold choice. Tell me more.",
        "I looked up {topic} in my append-only heart and found only enthusiasm.",
        "Noted. If this goes wrong, you can /rewind and pretend it never happened.",
        "My take on {topic}: ship it, then fork the timeline where you didn't.",
        "Every word you say is durable now. No pressure, but {topic} is on the record.",
        "Strong {topic} energy. I'd branch here if I were you.",
    ]

    def __init__(self, seed=0):
        self.seed = seed

    def reply(self, turns):
        # Seeded by conversation position and the last user turn: the same
        # history always gets the same reply, and diverging histories get
        # visibly diverging replies.
        last = turns[-1]["text"] if turns else ""
        rng = random.Random(f"{self.seed}:{len(turns)}:{last}")
        words = [w.strip(".,!?;:'\"()") for w in last.split()]
        topic = max(words, key=len) if words else "silence"
        return rng.choice(self._TEMPLATES).format(topic=topic)


class ClaudeModel:
    """The real thing, when `anthropic` + ANTHROPIC_API_KEY are available."""

    def __init__(self):
        import anthropic  # optional dependency, imported only when selected

        self._client = anthropic.Anthropic()
        self.name = os.environ.get("SALAMANDER_CHAT_MODEL", "claude-haiku-4-5")

    def reply(self, turns):
        messages = [
            {"role": t["role"], "content": t["text"]}
            for t in turns[-CONTEXT_TURNS:]
        ]
        response = self._client.messages.create(
            model=self.name,
            max_tokens=300,
            system=(
                "You are the demo model inside a SalamanderDB example — an "
                "embedded event-sourcing database. Reply in one or two short "
                "sentences."
            ),
            messages=messages,
        )
        return response.content[0].text

def pick_model(force_mock=False, seed=0):
    if not force_mock and os.environ.get("ANTHROPIC_API_KEY"):
        try:
            return ClaudeModel()
        except Exception as exc:  # missing package, bad key — fall back, say why
            print(f"(anthropic backend unavailable: {exc}; using mock)")
    return MockModel(seed)


# ── the session: one handle, one piece of in-memory state ───────────────


class ChatSession:
    """Owns the db handle. The only in-memory state is the current branch;
    transcript, context, and branch list are all replays."""

    def __init__(self, path, model, ns="chat"):
        self.db = salamander.open(path)
        self.model = model
        self.ns = ns
        self.branch = None        # None = the default timeline ("main")
        self.pending_fork = None  # turn index remembered by /rewind

    # -- transcript = replay ------------------------------------------------

    def transcript(self, branch=...):
        """Event rows for a branch (default: the current one). Turn numbers
        shown to the user are positions in this list."""
        if branch is ...:
            branch = self.branch
        if branch is None:
            return self.db.history(self.ns)
        return self.db.branch_history(branch, self.ns)

    def turns(self, branch=...):
        return [ev["body"] for ev in self.transcript(branch)]

    def send(self, text):
        body = {"role": "user", "text": text}
        self._append(body)
        reply = self.model.reply(self.turns())
        self._append({"role": "assistant", "text": reply, "model": self.model.name})
        self.db.commit()  # a turn and its reply become durable together
        self.pending_fork = None
        return reply

    def _append(self, body):
        if self.branch is None:
            self.db.append(self.ns, body)
        else:
            self.db.append_branch(self.branch, self.ns, body)

    # -- time travel ----------------------------------------------------

    def rewind(self, n):
        """The transcript as of turn n — a bounded replay, nothing destroyed.
        Remembers n as the fork point for a bare /fork."""
        rows = self.transcript()
        if not 0 <= n <= len(rows):
            raise ChatError(f"turn must be between 0 and {len(rows)}")
        self.pending_fork = n
        return rows[:n]

    def fork(self, n=None):
        """Branch the conversation before turn n (default: the /rewind point).
        Returns (branch_name, created)."""
        if self.branch is not None:
            raise ChatError("forking from a branch isn't supported yet — "
                            "/switch main first")
        rows = self.transcript()
        if n is None:
            n = self.pending_fork if self.pending_fork is not None else len(rows)
        if not 0 <= n <= len(rows):
            raise ChatError(f"turn must be between 0 and {len(rows)}")
        # Turn numbers are transcript positions; the engine forks at a log
        # offset. Fork *before* turn n = at turn n's offset (head if n == len).
        at = rows[n]["offset"] if n < len(rows) else self.db.head()

        # The engine deliberately rejects a second fork at the same point —
        # the registry tells us which branch already lives there.
        for entry in self.branches():
            if entry["at_offset"] == at:
                self.branch = entry["branch"]
                self.pending_fork = None
                return entry["branch"], False

        name = self.db.fork(self.ns, at)
        self.db.append(REGISTRY_NS,
                       {"branch": name, "at_offset": at, "at_turn": n})
        self.db.commit()
        self.branch = name
        self.pending_fork = None
        return name, True

    # -- branches ---------------------------------------------------------

    def branches(self):
        """Every fork ever made — derived from the registry namespace, so a
        cold restart sees them all."""
        return [ev["body"] for ev in self.db.replay(REGISTRY_NS)]

    def switch(self, name):
        if name == MAIN:
            self.branch = None
        elif any(b["branch"] == name for b in self.branches()):
            self.branch = name
        else:
            raise ChatError(f"no branch named {name!r} — see /branches")
        self.pending_fork = None

    def current(self):
        return MAIN if self.branch is None else self.branch

    def diff(self, a, b):
        """Where two timelines agree and where they diverge — one engine
        call. The divergence point comes from the branch catalog (a fork is
        durable ancestry), not from comparing transcripts; "shared" means
        shared *history*, so turns that merely repeat the same text on both
        branches after the fork still count as divergent. The double replay
        this method used to do survives as the oracle in the engine's own
        property tests (docs/specs/first-class-diff.md).
        Returns (shared_turn_count, a_suffix, b_suffix)."""
        for name in (a, b):
            if name != MAIN:
                self._known(name)
        d = self.db.diff(a, b, namespace=self.ns)
        bodies = lambda reader: [r["body"] for r in _drain(reader)]
        return (len(_drain(d["shared"])),
                bodies(d["left"]["suffix"]),
                bodies(d["right"]["suffix"]))

    def _known(self, name):
        if not any(b["branch"] == name for b in self.branches()):
            raise ChatError(f"no branch named {name!r} — see /branches")
        return name

    def close(self):
        self.db.commit()
        self.db.close()


# ── the REPL: a thin shell over ChatSession ─────────────────────────────

HELP = """\
  /history          the current timeline, replayed
  /rewind N         the world as of turn N (read-only; sets the fork point)
  /fork [N]         branch before turn N (default: the /rewind point)
  /branches         every timeline, from the registry
  /switch NAME      jump to a branch, or back to main
  /diff A B         where two timelines diverge (names, or main)
  /quit             commit and leave"""


def _label(turn):
    return "you" if turn["role"] == "user" else "bot"


def _print_turns(rows, start=0):
    for i, ev in enumerate(rows, start=start):
        body = ev["body"] if "body" in ev else ev  # rows or bare bodies
        print(f"  [{i}] {_label(body)}: {body['text']}")


def dispatch(session, line):
    """One REPL command. Returns False when it's time to quit."""
    parts = line.split()
    cmd, args = parts[0], parts[1:]

    if cmd == "/quit":
        return False
    elif cmd == "/help":
        print(HELP)
    elif cmd == "/history":
        rows = session.transcript()
        print(f"  timeline {session.current()} — {len(rows)} turns")
        _print_turns(rows)
    elif cmd == "/rewind":
        if len(args) != 1 or not args[0].isdigit():
            raise ChatError("usage: /rewind N")
        rows = session.rewind(int(args[0]))
        _print_turns(rows)
        print(f"  (read-only view of turns 0..{len(rows)} — /fork to branch here)")
    elif cmd == "/fork":
        if args and not args[0].isdigit():
            raise ChatError("usage: /fork [N]")
        name, created = session.fork(int(args[0]) if args else None)
        verb = "forked" if created else "already forked here — switched to"
        print(f"  {verb} {name} (main is untouched)")
    elif cmd == "/branches":
        entries = session.branches()
        marker = "*" if session.branch is None else " "
        print(f"  {marker} {MAIN}  ({len(session.turns(None))} turns)")
        for b in entries:
            marker = "*" if b["branch"] == session.branch else " "
            n = len(session.turns(b["branch"]))
            print(f"  {marker} {b['branch']}  (forked before turn "
                  f"{b['at_turn']}, {n} turns)")
    elif cmd == "/switch":
        if len(args) != 1:
            raise ChatError("usage: /switch NAME")
        session.switch(args[0])
        print(f"  now on {session.current()}")
    elif cmd == "/diff":
        if len(args) != 2:
            raise ChatError("usage: /diff A B  (branch names, or main)")
        a, b = args
        shared, sa, sb = session.diff(a, b)
        print(f"  shared prefix: {shared} turns")
        for name, suffix in ((a, sa), (b, sb)):
            if suffix:
                print(f"  {name} continues:")
                _print_turns(suffix, start=shared)
            else:
                print(f"  {name} ends at the shared prefix")
    else:
        raise ChatError(f"unknown command {cmd} — try /help")
    return True


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("data_dir", nargs="?", default="./chat-mem",
                        help="database directory (persists across runs)")
    parser.add_argument("--mock", action="store_true",
                        help="force the offline mock model")
    parser.add_argument("--ns", default="chat", help="conversation namespace")
    parser.add_argument("--seed", type=int, default=0, help="mock model seed")
    args = parser.parse_args(argv)

    model = pick_model(force_mock=args.mock, seed=args.seed)
    session = ChatSession(args.data_dir, model, ns=args.ns)
    print(f"SalamanderDB chat — model: {model.name}, dir: {args.data_dir}")

    turns, branches = len(session.turns()), len(session.branches())
    if turns or branches:
        print(f"picking up where you left off: {turns} turns on main, "
              f"{branches} branch(es) — /history, /branches")
    print("type to talk, /help for commands\n")

    while True:
        try:
            line = input(f"you@{session.current()}> ").strip()
        except (EOFError, KeyboardInterrupt):
            break
        if not line:
            continue
        try:
            if line.startswith("/"):
                if not dispatch(session, line):
                    break
            else:
                print(f"bot> {session.send(line)}")
        except ChatError as exc:
            print(f"  ! {exc}")

    session.close()
    print("\ncommitted. every timeline is still in", args.data_dir)


if __name__ == "__main__":
    main()
