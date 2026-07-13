"""pytest for the LangGraph checkpointer (WP-5 step 8e).

    python -m pytest examples/py/test_langgraph.py -v
"""

from langgraph_demo import build
from salamander_langgraph import SalamanderSaver


def _thread(tid):
    return {"configurable": {"thread_id": tid}}


def test_checkpoint_survives_restart(tmp_path):
    path = str(tmp_path / "cp")
    config = _thread("t1")

    saver = SalamanderSaver(path)
    graph = build(saver)
    graph.invoke({"log": []}, config)
    assert graph.get_state(config).values["log"] == ["plan", "act", "review"]
    n_checkpoints = len(list(graph.get_state_history(config)))
    assert n_checkpoints >= 3
    saver.close()  # release the single-writer lock, as if the process exited

    # A brand-new saver over the SAME directory recovers everything from disk.
    saver2 = SalamanderSaver(path)
    graph2 = build(saver2)
    assert graph2.get_state(config).values["log"] == ["plan", "act", "review"]
    assert len(list(graph2.get_state_history(config))) == n_checkpoints
    saver2.close()


def test_state_history_and_time_travel(tmp_path):
    with SalamanderSaver(str(tmp_path / "cp")) as saver:
        graph = build(saver)
        config = _thread("t1")
        graph.invoke({"log": []}, config)

        history = list(graph.get_state_history(config))  # newest first
        assert len(history) >= 3

        # Fetch the oldest checkpoint by its own config — time-travel.
        oldest = history[-1]
        past_state = graph.get_state(oldest.config)
        assert len(past_state.values.get("log", [])) < 3


def test_threads_are_isolated(tmp_path):
    with SalamanderSaver(str(tmp_path / "cp")) as saver:
        graph = build(saver)
        graph.invoke({"log": []}, _thread("a"))
        graph.invoke({"log": ["seed"]}, _thread("b"))

        assert graph.get_state(_thread("a")).values["log"] == ["plan", "act", "review"]
        assert graph.get_state(_thread("b")).values["log"] == [
            "seed",
            "plan",
            "act",
            "review",
        ]
