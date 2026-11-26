"""What a node sees of the run while one of its activations runs: ``ctx``."""
from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, Callable

from ..base import Context, Graph, Protocols

if TYPE_CHECKING:
    from .engine import Engine
    from .recorder import Act
    from .wiring import State


class RunContext(Context):
    __slots__ = ("node", "round", "problems", "resumed", "_engine", "_state", "_cause", "_act", "_cut", "_saved")

    def __init__(self, engine: "Engine", state: "State", cause: tuple, act: "Act | None" = None):
        self.node, self.round, self.problems = state.node.id, state.round, []
        self._cut = False       # the node has been told the run is stopping: what it did may not be all of it
        self._saved = False     # it has saved its progress at least once (ctx.commit)
        self.resumed = False    # it goes on from progress an earlier activation saved
        self._engine, self._state, self._cause, self._act = engine, state, cause, act

    def emit(self, port: str, payload: Any) -> str:
        return self._engine._emit(self._state, port, payload, self.round, self._cause, self._act)

    def correct(self, sent: str, payload: Any) -> str:
        return self._engine._correct(self._state, sent, payload, self.round, self._cause, self._act)

    def replaces(self, port: str) -> str | None:
        return self._state.replaced.get(port)

    def challengeable(self, port: str, which: str | None = None) -> bool:
        return self._engine._challengeable(self._state, port, which)

    def taken(self, port: str) -> list:
        return self._engine._taken_on(self._state, port)

    def reject(self, port: str, reason: str, which: str | None = None) -> dict:
        return self._engine._reject(self._state, self, port, reason, which)

    async def areject(self, port: str, reason: str, which: str | None = None) -> dict:
        return await self._engine._areject(self._state, self, port, reason, which)

    def rejections(self) -> list:
        return self._engine._rejections(self._state, self)

    def failures(self) -> list:
        return self._engine._failures(self._state, self)

    def explain(self, rejection: str, text: str) -> None:
        self._engine._explain(self._state, rejection, text)

    def call(self, port: str, payload: Any) -> Any:
        return self._engine._call(self._state, port, payload, self._act)

    async def acall(self, port: str, payload: Any) -> Any:
        return await self._engine._acall(self._state, port, payload, self._act)

    def stopping(self) -> bool:
        if self._state.halt or self._engine._stopping():
            self._cut = True
            return True
        return False

    def paused(self) -> bool:
        state = self._state
        return state.status == "paused" and not state.halt

    def time_left(self) -> float | None:
        state = self._state
        if state.time_limit is None or state.time_limit == float("inf"):
            return None
        return max(0.0, state.since + state.time_limit - time.monotonic())

    def commit(self) -> bool:
        return self._engine._commit(self._state, self)

    def receive(self, ports=None, timeout: float | None = None) -> dict | None:
        return self._engine._receive(self._state, self, ports, timeout)

    async def areceive(self, ports=None, timeout: float | None = None) -> dict | None:
        return await self._engine._areceive(self._state, self, ports, timeout)

    def checkpoint(self) -> bool:
        return self._engine._checkpoint(self._state, self)

    async def acheckpoint(self) -> bool:
        return await self._engine._acheckpoint(self._state, self)

    def team(self) -> dict:
        return self._engine._team(self.node)

    def control(self, nodes, action: str, *, team: bool = False) -> list:
        return self._engine._control(nodes, action, team, self.node)

    def engine(self, graph: Graph, protocols: Protocols | None = None, *, workers: int = 0, firm: bool = False) -> "Engine":
        return self._engine._inner(self, graph, protocols, workers, firm)

    def change(self, change: Any) -> None:
        self._engine._change(self._state, change, self._act)

    def progress(self, message: str) -> None:
        if not isinstance(message, str) or not message.strip() or len(message) > 1024:
            raise ValueError("progress requires a nonempty message of at most 1024 characters")
        engine = self._engine
        record = {"kind": "progress", "node": self.node, "round": self.round, "message": message}
        with engine._lock:
            engine._check_current(self._state)
            if engine._recorder is not None:
                engine._recorder.write(record)
        if engine._observe is not None:
            engine._tell("progress", node=self.node, round=self.round, message=message)

    def use(self, **amounts: float) -> None:
        self._engine._use(self._state, self.round, amounts)

    def once(self, name: str, do: Callable[[], Any]) -> Any:
        return self._engine._once(self._state, self.round, name, do)

    @property
    def retry_authorization(self) -> int:
        recorder = self._engine._recorder
        return getattr(recorder.journal, "retry_authorization", 0) if recorder else 0

    @property
    def key(self) -> str:
        return f"{self._engine._id}/{self.node}#{self.round}"

    @property
    def within(self) -> tuple:
        return self._engine._within
