"""Run a LangGraph agent whose memory is SalamanderDB, then RESTART it and
show the checkpoint history survived on disk.

    python examples/py/langgraph_demo.py
"""

import os
import shutil
import tempfile
from typing import TypedDict

from langgraph.graph import END, START, StateGraph

from salamander_langgraph import SalamanderSaver


class State(TypedDict):
    log: list


def _step(name: str):
    def run(state: State):
        return {"log": state["log"] + [name]}

    return run


def build(saver):
    g = StateGraph(State)
    g.add_node("plan", _step("plan"))
    g.add_node("act", _step("act"))
    g.add_node("review", _step("review"))
    g.add_edge(START, "plan")
    g.add_edge("plan", "act")
    g.add_edge("act", "review")
    g.add_edge("review", END)
    return g.compile(checkpointer=saver)


def main():
    path = os.path.join(tempfile.gettempdir(), "salamander-langgraph-demo")
    shutil.rmtree(path, ignore_errors=True)
    config = {"configurable": {"thread_id": "run-1"}}

    # ── run the agent; each node transition is checkpointed to disk ─────
    saver = SalamanderSaver(path)
    graph = build(saver)
    result = graph.invoke({"log": []}, config)
    print("final state:", result)

    checkpoints = list(graph.get_state_history(config))
    print(f"checkpoints written: {len(checkpoints)}")
    saver.close()  # release the single-writer lock, as if the process exited

    # ── restart: a brand-new saver over the SAME directory ──────────────
    print("\n--- restart (reopen the same directory) ---")
    saver2 = SalamanderSaver(path)
    graph2 = build(saver2)
    recovered = graph2.get_state(config)
    print("recovered state:", recovered.values)
    print("recovered checkpoints:", len(list(graph2.get_state_history(config))))
    saver2.close()

    print("\nThe agent's memory outlived the process — the log is the DB.")


if __name__ == "__main__":
    main()
