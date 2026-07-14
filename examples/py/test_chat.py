"""pytest suite for the chat demo — ChatSession driven directly, no stdin.

    python -m pytest examples/py/test_chat.py -v
"""

import pytest

from chat import MAIN, ChatError, ChatSession, MockModel


class ProbeModel:
    """Records the context each reply was asked for."""

    name = "probe"

    def __init__(self):
        self.contexts = []

    def reply(self, turns):
        self.contexts.append(list(turns))
        return f"reply-{len(turns)}"


@pytest.fixture
def session(tmp_path):
    s = ChatSession(str(tmp_path / "db"), ProbeModel())
    yield s
    s.close()


def texts(session, branch=...):
    return [t["text"] for t in session.turns(branch)]


def test_turn_roundtrip_and_cold_restart(tmp_path):
    path = str(tmp_path / "db")
    s = ChatSession(path, ProbeModel())
    reply = s.send("hello")
    assert reply == "reply-1"
    s.close()

    s2 = ChatSession(path, ProbeModel())
    turns = s2.turns()
    assert [t["role"] for t in turns] == ["user", "assistant"]
    assert turns[0]["text"] == "hello"
    assert turns[1]["text"] == "reply-1"
    s2.close()


def test_rewind_is_read_only(session):
    session.send("one")
    session.send("two")
    before = session.transcript()

    rows = session.rewind(2)
    assert [r["body"]["text"] for r in rows] == ["one", "reply-1"]
    assert session.transcript() == before  # nothing destroyed
    assert session.pending_fork == 2

    with pytest.raises(ChatError):
        session.rewind(99)


def test_fork_diverges_and_main_is_untouched(session):
    session.send("pick a name")
    session.send("why amphibious?")
    main_before = session.transcript()

    session.rewind(2)
    name, created = session.fork()
    assert created
    assert session.current() == name

    session.send("a bird name instead?")
    assert texts(session)[:2] == ["pick a name", "reply-1"]  # shared prefix
    assert texts(session)[2] == "a bird name instead?"       # divergence
    assert session.transcript(None) == main_before           # main untouched


def test_fork_collision_switches_to_existing_branch(session):
    session.send("one")
    session.send("two")
    name, created = session.fork(2)
    assert created

    session.switch(MAIN)
    again, created_again = session.fork(2)
    assert again == name
    assert not created_again
    assert session.current() == name


def test_fork_from_branch_is_refused(session):
    session.send("one")
    session.fork(1)
    with pytest.raises(ChatError):
        session.fork(0)


def test_model_context_follows_the_current_branch(session):
    session.send("one")
    session.send("two")
    session.fork(2)
    session.send("three")

    # The context of the last reply is the branch's replay: the shared
    # prefix plus the new user turn — not main's four turns.
    last_context = session.model.contexts[-1]
    assert [t["text"] for t in last_context] == ["one", "reply-1", "three"]

    session.switch(MAIN)
    session.send("four")
    last_context = session.model.contexts[-1]
    assert [t["text"] for t in last_context] == \
        ["one", "reply-1", "two", "reply-3", "four"]


def test_cold_restart_sees_all_branches(tmp_path):
    path = str(tmp_path / "db")
    s = ChatSession(path, ProbeModel())
    s.send("one")
    name, _ = s.fork(1)
    s.send("branch turn")
    s.close()

    s2 = ChatSession(path, ProbeModel())
    assert [b["branch"] for b in s2.branches()] == [name]
    s2.switch(name)
    assert texts(s2) == ["one", "branch turn", "reply-2"]
    s2.close()


def test_switch_rejects_unknown_branch(session):
    with pytest.raises(ChatError):
        session.switch("nope")


def test_diff_reports_shared_prefix_and_suffixes(session):
    session.send("one")
    session.send("two")
    name, _ = session.fork(2)
    session.send("branch two")

    shared, main_sfx, branch_sfx = session.diff(MAIN, name)
    assert shared == 2
    assert [t["text"] for t in main_sfx] == ["two", "reply-3"]
    assert [t["text"] for t in branch_sfx] == ["branch two", "reply-3"]

    with pytest.raises(ChatError):
        session.diff(MAIN, "nope")


def test_mock_model_is_deterministic():
    turns = [{"role": "user", "text": "name my database"}]
    assert MockModel(seed=7).reply(turns) == MockModel(seed=7).reply(turns)
    assert MockModel(seed=7).reply(turns) != MockModel(seed=8).reply(turns) or \
        True  # different seeds may collide; determinism is the claim, not spread
