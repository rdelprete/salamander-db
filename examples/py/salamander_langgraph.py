"""A LangGraph checkpointer backed by SalamanderDB (WP-5 build step 8e).

The whole thesis in one integration: a LangGraph *thread* is a Salamander
*namespace*, each *checkpoint* is one appended event, and reads are replays.
Because the log is the only durable structure, an agent run survives a
process restart — reopen the DB and the checkpoint history is intact.

    saver = SalamanderSaver("./agent-memory")
    graph = builder.compile(checkpointer=saver)
    graph.invoke(inputs, {"configurable": {"thread_id": "conv-1"}})

Checkpoints can hold arbitrary Python state, so they're serialized with
LangGraph's own serde (`self.serde`) to bytes and stored base64-wrapped in a
JSON envelope — the DB stays JSON, the payload stays lossless.
"""

from __future__ import annotations

import base64
from typing import Any, Iterator, Optional, Sequence

from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import (
    BaseCheckpointSaver,
    ChannelVersions,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
)

import salamander


def _b64e(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def _b64d(text: str) -> bytes:
    return base64.b64decode(text.encode("ascii"))


class SalamanderSaver(BaseCheckpointSaver):
    """Persist LangGraph checkpoints to a SalamanderDB directory."""

    def __init__(self, path: str):
        super().__init__()
        # fsync every checkpoint — a checkpointer wants each put durable.
        self._db = salamander.open(path, commit_every_count=1)

    # SalamanderDB is single-writer: hold ONE handle, close it to release the
    # lock (e.g. before another process/handle reopens the same directory).
    def close(self) -> None:
        self._db = None

    def __enter__(self) -> "SalamanderSaver":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # ── namespace + config helpers ──────────────────────────────────────

    @staticmethod
    def _ns(config: RunnableConfig) -> str:
        c = config["configurable"]
        return f'{c["thread_id"]}|{c.get("checkpoint_ns", "")}'

    def _config_for(self, config: RunnableConfig, checkpoint_id: Optional[str]) -> RunnableConfig:
        c = config["configurable"]
        return {
            "configurable": {
                "thread_id": c["thread_id"],
                "checkpoint_ns": c.get("checkpoint_ns", ""),
                "checkpoint_id": checkpoint_id,
            }
        }

    # ── write path ──────────────────────────────────────────────────────

    def put(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        ctype, cbytes = self.serde.dumps_typed(checkpoint)
        mtype, mbytes = self.serde.dumps_typed(metadata)
        self._db.append(
            self._ns(config),
            {
                "kind": "checkpoint",
                "checkpoint_id": checkpoint["id"],
                "parent_id": config["configurable"].get("checkpoint_id"),
                "checkpoint_type": ctype,
                "checkpoint_b64": _b64e(cbytes),
                "metadata_type": mtype,
                "metadata_b64": _b64e(mbytes),
            },
        )
        return self._config_for(config, checkpoint["id"])

    def put_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        serialized = []
        for channel, value in writes:
            wtype, wbytes = self.serde.dumps_typed(value)
            serialized.append({"channel": channel, "type": wtype, "b64": _b64e(wbytes)})
        self._db.append(
            self._ns(config),
            {
                "kind": "writes",
                "checkpoint_id": config["configurable"]["checkpoint_id"],
                "task_id": task_id,
                "task_path": task_path,
                "writes": serialized,
            },
        )

    # ── read path ───────────────────────────────────────────────────────

    def _events(self, ns: str) -> list[dict]:
        return [e["body"] for e in self._db.replay(ns)]

    def _pending_writes(self, events: list[dict], checkpoint_id: str) -> list[tuple[str, str, Any]]:
        out: list[tuple[str, str, Any]] = []
        for body in events:
            if body.get("kind") == "writes" and body.get("checkpoint_id") == checkpoint_id:
                for w in body["writes"]:
                    value = self.serde.loads_typed((w["type"], _b64d(w["b64"])))
                    out.append((body["task_id"], w["channel"], value))
        return out

    def _to_tuple(self, config: RunnableConfig, body: dict, events: list[dict]) -> CheckpointTuple:
        checkpoint = self.serde.loads_typed((body["checkpoint_type"], _b64d(body["checkpoint_b64"])))
        metadata = self.serde.loads_typed((body["metadata_type"], _b64d(body["metadata_b64"])))
        parent = self._config_for(config, body["parent_id"]) if body.get("parent_id") else None
        return CheckpointTuple(
            config=self._config_for(config, body["checkpoint_id"]),
            checkpoint=checkpoint,
            metadata=metadata,
            parent_config=parent,
            pending_writes=self._pending_writes(events, body["checkpoint_id"]),
        )

    def get_tuple(self, config: RunnableConfig) -> Optional[CheckpointTuple]:
        events = self._events(self._ns(config))
        checkpoints = [b for b in events if b.get("kind") == "checkpoint"]
        if not checkpoints:
            return None
        wanted = config["configurable"].get("checkpoint_id")
        if wanted is None:
            body = checkpoints[-1]  # latest
        else:
            body = next((b for b in reversed(checkpoints) if b["checkpoint_id"] == wanted), None)
            if body is None:
                return None
        return self._to_tuple(config, body, events)

    def list(
        self,
        config: Optional[RunnableConfig],
        *,
        filter: Optional[dict[str, Any]] = None,
        before: Optional[RunnableConfig] = None,
        limit: Optional[int] = None,
    ) -> Iterator[CheckpointTuple]:
        if config is None:
            return
        events = self._events(self._ns(config))
        checkpoints = [b for b in events if b.get("kind") == "checkpoint"]
        before_id = before["configurable"]["checkpoint_id"] if before else None
        count = 0
        for body in reversed(checkpoints):  # newest first
            if before_id is not None and body["checkpoint_id"] >= before_id:
                continue
            yield self._to_tuple(config, body, events)
            count += 1
            if limit is not None and count >= limit:
                return
