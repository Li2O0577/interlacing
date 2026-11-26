"""Recovery state for the opt-in SQLite delivery protocol."""
from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING

from ..base import NodeError, canonical
from .journal import pack_body, unpack_body
from .recorder import _envelope
from .rework import Rework
from .sqlite import SQLiteJournal

if TYPE_CHECKING:
    from .engine import Engine


def payload_key(payload) -> str:
    if isinstance(payload, (bytes, bytearray, memoryview)):
        return "bytes:" + hashlib.sha256(bytes(payload)).hexdigest()
    return "json:" + hashlib.sha256(canonical(pack_body(payload)).encode("utf-8")).hexdigest()


def copy_body(body):
    """Use the same value representation in memory and in the journal."""
    return unpack_body(json.loads(json.dumps(pack_body(body), allow_nan=False)))


class DurableState:
    """Fold committed journal events into the state needed to resume the engine."""

    def __init__(self, checkpoint: dict | None = None):
        self.data = checkpoint if checkpoint is not None else {
            "seq": 0, "run": None, "ended": False, "changes": [], "rounds": {}, "switches": {},
            "pending": {}, "mail": {}, "counters": {}, "emitted": {}, "outputs": {}, "snaps": {},
            "limited": {}, "used": {}, "once": {}, "problems": [], "envelopes": 0,
        }
        self.rework = Rework(self.data.setdefault("rework", {
            "messages": {}, "deliveries": {}, "readings": {}, "readers": {}, "waiting": {},
        }))

    @classmethod
    def load(cls, journal: SQLiteJournal, *, at: int | None = None) -> tuple[int | None, DurableState]:
        checkpoint, records = journal.recovery(at=at, required="rework")
        position = checkpoint["seq"] if checkpoint is not None else None
        state = cls(checkpoint)
        for record in records:
            state.apply(record)
        return position, state

    @property
    def seq(self) -> int:
        return self.data["seq"]

    @property
    def pending(self) -> dict:
        return self.data["pending"]

    def apply(self, record: dict) -> None:
        data = self.data
        kind = record["kind"]
        data["seq"] += 1
        if kind != "fork" or record["sent"]:
            data["ended"] = kind == "end"
        if kind == "run":
            if record.get("durable") is not True:
                raise NodeError("this journal was created without durable delivery")
            data["run"] = {key: record[key] for key in ("id", "graph")}
            self._deliver(record["sent"])
        elif kind == "fork":
            for packed in record["sent"]:
                if len(packed) > 5:
                    del data["mail"][packed[5]]
            self._deliver(record["sent"])
        elif kind == "bind":
            node = record["node"]
            data["rounds"][node] = record["round"]
            self.pending[node] = {"round": record["round"], "took": record["took"], "ops": [], "resumed": False}
            self.rework.bind(record)
            if "redo" in record:
                self.pending[node].update(redo=record["redo"], redo_outputs=record["redo_outputs"])
        elif kind == "deliver":
            pending = self.pending[record["node"]]
            pending["ops"].append({key: record[key] for key in ("kind", "port", "key", "token")})
            self._deliver(record["sent"])
            self.rework.emit(record, pending)
            counters = data["emitted"].setdefault(record["node"], {})
            counters[record["port"]] = int(record["token"].rsplit("#", 1)[1])
            for name, _, body in record["outputs"]:
                data["outputs"].setdefault(name, []).append(body)
            data["limited"].update(dict.fromkeys(record["limited"]))
        elif kind == "correct":
            self.pending[record["node"]]["ops"].append({key: record[key] for key in ("kind", "key", "token")})
            self.rework.revise(record)
            for name, index, body in record["outputs"]:
                data["outputs"][name][index] = body
            for packed, route in zip(record["sent"], record["routes"]):
                edge, seq = packed[:2]
                data["counters"][edge] = seq
                data["envelopes"] += 1
                if route == "unread":
                    old, new = packed[5], f"{edge}#{seq}"
                    data["mail"] = {new if id == old else id: packed if id == old else body
                                    for id, body in data["mail"].items()}
                elif route == "tell":
                    data["mail"][f"{edge}#{seq}"] = packed
            data["limited"].update(dict.fromkeys(record["limited"]))
        elif kind == "topology":
            operations = record["operations"]
            data["changes"].append(operations)
            node = record["node"]
            self.pending[node]["ops"].append({"kind": kind, "operations": operations})
            self.rework.reading(node, self.pending[node])["replayable"] = False
        elif kind == "act":
            node = record["node"]
            pending = self.pending[node]
            if "redo" not in pending:
                for id in pending["took"]:
                    data["mail"].pop(id, None)
            if not record.get("stale"):
                data["snaps"].update(record["snaps"])
                if node not in record["snaps"]:
                    data["snaps"].pop(node, None)
            data["problems"].extend(record.get("problems", ()))
            self.rework.settle(record, pending)
            if record.get("open"):
                pending.update(took=[], ops=[], resumed=True)
            else:
                del self.pending[node]
        elif kind == "control":
            data["switches"].update(dict.fromkeys(record["nodes"], record["action"]))
        elif kind == "use":
            for name, value in record["amounts"].items():
                data["used"][name] = data["used"].get(name, 0) + value
        elif kind == "once":
            data["once"].setdefault(record["node"], {}).setdefault(str(record["round"]), {})[record["name"]] = record["value"]

    def _deliver(self, envelopes: list) -> None:
        for packed in envelopes:
            edge, seq = packed[:2]
            self.data["mail"][self.rework.store(packed)] = packed
            self.data["counters"][edge] = seq
            self.data["envelopes"] += 1

    def rebuild(self, engine: Engine, switched: dict[str, str]) -> None:
        data = self.data
        for change in data["changes"]:
            engine._remake(change)
        for edge, seq in data["counters"].items():
            engine._feeds[edge].sent = seq
        for id, state in engine._states.items():
            state.round = data["rounds"].get(id, 0)
            state.started = state.round > 0
            state.status = switched.get(data["switches"].get(id, ""), state.status)
            state.emitted.update(data["emitted"].get(id, {}))
            if id in data["snaps"]:
                state.node.restore(data["snaps"][id])
            state.bound = id in self.pending
            state.rework = id in self.rework.waiting
        for packed in data["mail"].values():
            feed = engine._feeds[packed[0]]
            engine._sched.place(feed, _envelope(feed, packed))
        engine._sched.envelopes = data["envelopes"]
        engine._ledger.meter.update(data["used"])
        for node, rounds in data["once"].items():
            for round, kept in rounds.items():
                for name, value in kept.items():
                    engine._ledger.keep(node, int(round), name, value)
        engine._result.activations = sum(data["rounds"].values())
        for name, values in data["outputs"].items():
            engine._result.outputs[name] = [unpack_body(value) for value in values]
        engine._result.problems.extend(data["problems"])
        engine._limited.update(data["limited"])
