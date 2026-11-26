"""What the run writes to its journal, and how a run is rebuilt from it.

The records themselves are described in ``journal.py``. This module knows what goes into each one and how
to put the run back as it was: counters, the nodes' own state and the envelopes still waiting.
"""
from __future__ import annotations

from collections import OrderedDict
from collections.abc import Iterable
import secrets
import time
from typing import Any, Callable, Mapping

from ..base import Envelope, Protocol
from .journal import JournalBackend, history_record, pack, pack_body, unpack_body
from .ledger import Ledger
from .result import Result
from .scheduler import Scheduler
from .wiring import ACTIVE, PAUSED, STOPPED, Reading, Sent, Wiring, bounded


class Act:
    """What one activation did, for the journal."""

    __slots__ = ("node", "round", "took", "sent", "out", "out_indices", "limited", "calls", "snaps", "problems", "changes", "emits",
                 "fixed_box", "fixed_out", "redos", "rejects", "started")

    def __init__(self, node: str, round: int, took: tuple) -> None:
        self.started = time.monotonic()
        self.node, self.round, self.took = node, round, took
        self.sent: list[Envelope] = []
        self.out: list[tuple] = []
        self.out_indices: list[int] = []  # output slots assigned at emission, even if records finish out of order
        self.limited: list[str] = []
        self.calls: list[str] = []
        self.snaps: dict[str, Any] = {}
        self.problems: list[dict] = []
        self.changes: list = []           # the changes to the graph it made, as plain data
        self.emits: list = []             # [name, port, [[feed, envelope id]...], [[output, index]...]] per message
        self.redos: list[str] = []        # new envelopes that revise a previously consumed input
        self.fixed_box: list[str] = []    # envelopes not yet read that a correction took the place of
        self.fixed_out: list[tuple] = []  # (graph output, index, payload) a correction changed
        self.rejects: list[str] = []      # the edges on which it rejected what came


class Recorder:
    """Writes one run's records to its journal."""

    __slots__ = ("journal",)

    def __init__(self, journal: JournalBackend):
        self.journal = journal

    def read(self) -> list[dict]:
        return self.journal.read()

    def _write(self, record: dict) -> None:
        self.journal.write({"schema_version": 1, **record})

    def run(self, id: str, graph: str, sent: list[Envelope]) -> None:
        self._write({"kind": "run", "id": id, "scope_id": secrets.token_hex(16), "graph": graph,
                     "sent": [pack(e) for e in sent]})

    def act(self, act: Act, open: bool = False, reading: Reading | None = None,
            children: Iterable[JournalBackend] = ()) -> None:
        """What an activation did. ``open``: it saved its progress and goes on (``ctx.commit``)."""
        child_history = []
        for child in children:
            events = [history_record(record, seq) for seq, record in enumerate(child.read(), 1)
                      if record["kind"] in ("run", "act")]
            if events:
                child_history.append(events)
        self._write({"kind": "act", "node": act.node, "round": act.round, "took": list(act.took),
                     "elapsed_seconds": time.monotonic() - act.started,
                     **({"rerun_outputs": reading.emits} if reading is not None else {}),
                     "sent": [pack(e) for e in act.sent],
                     "out": [[name, pack_body(payload)] for name, payload in act.out],
                     **({"out_indices": act.out_indices} if act.out_indices else {}),
                     "limited": act.limited, "calls": act.calls, "snaps": act.snaps,
                     **({"problems": act.problems} if act.problems else {}),
                     **({"changes": act.changes} if act.changes else {}),
                     **({"emits": act.emits} if act.emits else {}),
                     **({"redos": act.redos} if act.redos else {}),
                     **({"fixed": act.fixed_box} if act.fixed_box else {}),
                     **({"rejects": act.rejects} if act.rejects else {}),
                     **({"fixed_out": [[name, index, pack_body(payload)] for name, index, payload in act.fixed_out]}
                        if act.fixed_out else {}),
                     **({"child_history": child_history} if child_history else {}),
                     **({"open": True} if open else {})})

    def use(self, node: str, round: int, amounts: Mapping[str, float]) -> None:
        self._write({"kind": "use", "node": node, "round": round, "amounts": dict(amounts)})

    def once(self, node: str, round: int, name: str, value: Any) -> None:
        self._write({"kind": "once", "node": node, "round": round, "name": name, "value": value})

    def control(self, nodes: list[str], action: str, by: str | None = None) -> None:
        self._write({"kind": "control", "nodes": nodes, "action": action, **({"by": by} if by else {})})

    def review(self, kind, rejection) -> None:
        self._write({"kind": kind, "id": rejection.id, "node": rejection.by, "edge": rejection.feed.id,
                     **({"answer": rejection.answer} if rejection.answer else {"reason": rejection.reason})})

    def change(self, under: str, graph: str, dropped: int) -> None:
        self._write({"kind": "change", "from": under, "graph": graph, "dropped": dropped})

    def end(self) -> None:
        self._write({"kind": "end"})
        self.journal.prune_empty_inner()

    def close(self) -> None:
        self.journal.close()

    def inner(self, node: str, round: int) -> JournalBackend:
        """The journal of a run made inside activation ``round`` of ``node``."""
        return self.journal.inner(node, round)


SWITCHED = {"pause": PAUSED, "stop": STOPPED, "resume": ACTIVE, "restart": ACTIVE}


def under(records: list[dict]) -> str | None:
    """The digest of the graph the last part of the run was written under."""
    return next((r["graph"] for r in reversed(records) if r["kind"] in ("run", "change")), None)


def _envelope(feed, packed) -> Envelope:
    edge, seq, round, body, cause, *replaces = packed
    return Envelope(f"{edge}#{seq}", edge, seq, round, unpack_body(body, feed.protocol, feed.port.port),
                    "", tuple(cause), *replaces)


def _reading(record, state, wiring, sent) -> Reading | None:
    """Rebuild ordinary value inputs from already journaled bodies, without running application decoders."""
    reading = Reading({}, {})
    for name in record["took"]:
        edge, seq = name.rsplit("#", 1)
        feed = wiring.feeds.get(edge)
        packed = sent.get(edge, {}).get(int(seq))
        if (feed is None or feed.port.state is not state or packed is None or feed.stream
                or (feed.protocol is not None and type(feed.protocol).decode is not Protocol.decode)):
            return None
        port = feed.port.port
        envelope = _envelope(feed, packed)
        items = reading.took.setdefault(port.name, [])
        reading.at[name] = (port.name, len(items) if port.many else None)
        items.append((feed, envelope))
        if port.many:
            reading.inputs.setdefault(port.name, []).append(envelope.body)
        else:
            reading.inputs[port.name] = envelope.body
    reading.emits = record["rerun_outputs"]
    return reading


def restore(records: list[dict], wiring: Wiring, scheduler: Scheduler, ledger: Ledger, result: Result,
            limited: dict[str, None], change: Callable[[list], int], *, same_graph: bool = True) -> tuple[str | None, set[str], int]:
    """Rebuild counters, node state and waiting envelopes from the journal. Returns the run's id, the
    envelopes already dealt with whose sender must run again, and how many waiting envelopes were dropped
    because their edge is not in this graph. ``change`` makes again a change an activation made to the graph."""
    states = wiring.states
    run_id: str | None = None
    sent: dict[str, dict[int, list]] = {}
    took: set[str] = set()
    fixed: set[str] = set()         # envelopes a correction took the place of before they were read
    snaps: dict[str, Any] = {}
    going: set[str] = set()         # nodes whose last activation saved its progress and had not ended
    histories: dict[str, OrderedDict] = {}
    redos: set[str] = set()
    for record in records:
        kind = record["kind"]
        if kind == "change" or record.get("changes"):
            histories.clear()
        if kind == "run":
            run_id = run_id or record.get("id") or record["graph"][:16]
        elif kind == "use":
            ledger.add(record["amounts"])
        elif kind == "once":
            ledger.keep(record["node"], record["round"], record["name"], record["value"])
        elif kind == "control":
            for id in record["nodes"]:
                state = states.get(id)
                if state is not None:
                    state.status = SWITCHED[record["action"]]
                if record["action"] == "restart":
                    snaps.pop(id, None)         # it started over: what it held before is gone
                    going.discard(id)
        if kind == "act":
            for described in record.get("changes", ()):
                change(described)
            history = histories.setdefault(record["node"], OrderedDict())
            for name in record["took"]:
                bounded(history, name, record if same_graph and "rerun_outputs" in record else None)
            redos.update(record.get("redos", ()))
            took.update(record["took"])
            fixed.update(record.get("fixed", ()))
            for id in record.get("rejects", ()):
                if id in wiring.feeds:
                    wiring.feeds[id].rejected += 1
            for name, body in record["out"]:
                if name in result.outputs:
                    result.outputs[name].append(unpack_body(body))
            for name, index, body in record.get("fixed_out", ()):
                if name in result.outputs and index < len(result.outputs[name]):
                    result.outputs[name][index] = unpack_body(body)
            state = states.get(record["node"])
            if state is not None:
                for name, port, envelopes, outputs in record.get("emits", ()):
                    if name not in state.sent_log:
                        count = int(name.rsplit("#", 1)[1])
                        state.emitted[port] = max(state.emitted.get(port, 0), count)
                    bounded(state.sent_log, name, Sent(port, envelopes, outputs, record["round"]))
            for id in record["limited"]:
                if id in wiring.feeds:
                    limited[id] = None
            result.problems.extend(record.get("problems", ()))
            node = record["node"]
            for id in (node, *record["calls"]):
                state = states.get(id)
                if state is not None and not (id == node and node in going):    # one activation, however often saved
                    state.round += 1
                    state.started = True
                    result.activations += 1
            if record.get("open"):
                going.add(node)
            else:
                going.discard(node)
            scheduler.envelopes += len(record["calls"])
            snaps.update(record["snaps"])
        if kind in ("run", "act"):
            for packed in record["sent"]:
                sent.setdefault(packed[0], {})[packed[1]] = packed
    # A receiver can commit before its sender. Resolve envelope references only after indexing all sends.
    for id in took:
        feed = wiring.feeds.get(id.rsplit("#", 1)[0])
        if feed is not None:
            bounded(feed.port.state.read_log, id, None)
    for node, history in histories.items():
        state = states.get(node)
        if state is None:
            continue
        restored: dict[int, Reading | None] = {}
        for name, entry in history.items():
            reading = None
            if entry is not None:
                key = entry["round"]
                if key not in restored:
                    restored[key] = _reading(entry, state, wiring, sent)
                reading = restored[key]
            bounded(state.read_log, name, reading)
    took |= fixed
    dropped = 0
    orphans = took - {f"{feed}#{seq}" for feed, by_seq in sent.items() for seq in by_seq}
    for id, by_seq in sent.items():
        seqs = sorted(by_seq)
        # A single rerun may have consumed the last of several successive corrections.
        # Earlier revisions in that chain must not reappear as independent mailbox deliveries.
        for seq in reversed(seqs):
            name = f"{id}#{seq}"
            if name in redos and name in took:
                took.add(by_seq[seq][5])
        waiting = [seq for seq in seqs if f"{id}#{seq}" not in took]
        feed = wiring.feeds.get(id)
        if feed is None:
            dropped += len(waiting)
            continue
        feed.sent = seqs[-1]
        scheduler.envelopes += len(seqs)
        if feed.passive:
            waiting = seqs[-1:]
        elif feed.latest:
            waiting = [seq for seq in waiting if seq == seqs[-1] or f"{id}#{seq}" in redos]
        for seq in waiting:
            envelope = _envelope(feed, by_seq[seq])
            receiver = feed.port.state
            reading = receiver.read_log.get(envelope.replaces)
            if envelope.id in redos and reading is not None and receiver.node.on_correction == "rerun":
                del receiver.read_log[envelope.replaces]
                bounded(receiver.read_log, envelope.id, reading)
                reading.update(feed, envelope.replaces, envelope, envelope.body)
                if not reading.queued:
                    receiver.redo.append(reading)
                    reading.queued = True
            else:
                scheduler.place(feed, envelope)
    for id, snapshot in snaps.items():
        if id in states:
            states[id].node.restore(snapshot)
    for id in going:
        if id in states:
            states[id].resuming = True      # it goes on from the progress it saved
    return run_id, orphans, dropped
