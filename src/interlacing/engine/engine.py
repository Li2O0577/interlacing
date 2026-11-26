"""Event-driven execution of a graph.

Every edge (and every graph input) is a *feed* with its own mailbox. An input port is ready when all its
forward feeds hold an envelope, or when one of its back feeds does. A node keeps a count of its required ports
that are not ready; an arriving envelope changes that count for one node and nothing else, so scheduling costs
the same per message whatever the size of the graph.

A node is activated when

* it fires on ``ALL``: every required port is ready (a node that requires nothing starts once, by itself, and
  again whenever an optional port becomes ready);
* it fires on ``ANY``: any envelope arrives; each activation takes exactly one.

An activation takes one envelope from each feed of each ready port (a ``many`` port yields a list, in the
order of its edges), runs the node, and delivers what it emits. A node never runs twice at the same time. A
back edge stops carrying envelopes once it has carried ``max_rounds`` of them. The run ends when nothing is
running and nothing is ready.

Three kinds of edge are handled apart from that:

* a protocol that does not trigger (``triggers = False``) keeps only its newest envelope and never makes a
  node ready; whenever the node runs for another reason it finds that value on the port, and it stays there;
* a ``STREAM`` protocol delivers one envelope per sending activation, holding a ``Stream``: everything that
  activation emits on the port is a chunk of it, and it closes when the activation ends. With two or more
  workers the receiver reads while the sender is still writing; with fewer it reads once the sender is done;
* a ``CALL`` protocol is not scheduled at all. The sending node calls ``ctx.call(port, payload)``; the
  receiving node runs at once, in the caller's thread, and what it returns is the answer. A node that is
  called takes nothing else: it only ever runs when called, one call at a time.

``workers=0`` runs everything in the calling thread, in a fixed order. With more workers, nodes run in a pool
of threads and only the bookkeeping is serialised.

A node whose ``run`` is a coroutine (``Node.asynchronous``) is awaited instead. A graph that has one runs on an
event loop (``_loop``): the caller's, under ``arun``, or one of its own under ``run``. Coroutine activations
are awaited on it, without a thread each; the others run from start to end in threads, as they would without
a loop. ``workers`` still bounds how many activations are under way (one, when it is 0, in the same fixed
order). A coroutine activation that overruns is cancelled. Both drivers share every step of an activation:
``_pick``, ``_started``, ``_activate`` (or ``_aactivate``) and ``_ended``.

``stopping`` is a function the engine asks before each activation; once it returns true the run stops as if
``stop`` had been called. A graph that runs inside a node of another graph is stopped this way.

``journal`` makes the run durable: every finished activation is written to it in one piece, and ``resume``
on a new engine with the same journal takes the run up where it stopped. What a finished activation did is
never done again; an activation that was cut off runs again from its inputs, so a node may see the same
input twice, and what it had already sent on is not delivered a second time to a node that has dealt with
it. Everything that travels must then be plain data, a ``Ref`` or a stream of those. ``resume(change=True)``
continues under a different graph: nodes and edges that are still there keep their state and their waiting
envelopes, the rest is dropped.

``time_limit`` is the longest one activation may take, in seconds. A node's own ``time_limit`` attribute wins over
it (``math.inf`` for a node that may take as long as it likes, a person for instance). A thread cannot be
killed, so when an activation overruns, the run fails with that node named, running nodes see
``ctx.stopping()``, and the engine returns without waiting for the one that is stuck; whatever it does
afterwards is thrown away. A coroutine that overruns is cancelled. Nothing it did was journaled, so a resumed
run starts that activation again.

``observe`` is a function the engine tells what happens, as it happens: one mapping per event, with
``event`` one of ``started``, ``emitted``, ``finished``, ``failed`` (each with ``node`` and ``round``) and,
last, ``ended``. It is called from the threads that run the nodes and must not raise; it is how a screen
or a log follows a run without reading the journal.

What a node sends is checked as it is sent: by the port's schema, by the protocol of each edge it leaves
on, and by the port's own checks. When a check finds a problem the port decides (see ``Port``):

* while the port has repairs left in this activation, the node is told: ``emit`` raises ``Invalid`` with
  ``again`` true. A node that can put things right (an agent) does, and sends again. A node that lets the
  exception go is run again from the same inputs, with ``ctx.problems`` saying what was wrong, provided it
  had not yet delivered anything;
* then, a port that warns sends the payload on as it is and the problem is recorded;
* otherwise the node fails and the run stops, to be resumed once the cause is dealt with.

Every problem, whatever became of it, is in ``Result.problems``, in the journal, and told to ``observe``
(``rejected`` when it was given back to the node, ``warned`` when it was let through). The same applies on
arrival, when a protocol finds that what arrived is not what was sent: the receiving port fails or warns.

A node may run a whole graph inside one activation: ``ctx.engine(graph, protocols)`` gives it an engine
that belongs to the outer run. It stops with it, works under the same time limit, and tells the outer
observer its events with ``within`` naming the node (or nodes) it runs inside. When the outer run has a
journal, the inner one keeps its own next to it (``<journal>.d/<node>-<round>.jsonl``), so that after a
restart the outer activation runs again and the inner run goes on from where it was instead of starting
over. Problems found inside are recorded outside as well, under ``outer/inner``.

``transport`` stands between sending and receiving: every envelope body passes through its ``carry``. Without
one, bodies are handed over as they are. Streams and calls do not pass through it yet.

The engine itself only drives the run; each part of the work is done by a piece of its own:

* ``wiring``: the mailboxes, ports and node states a graph becomes;
* ``scheduler``: putting envelopes in, taking them out, and which nodes are ready;
* ``checks``: what is checked on the way out and in, and what follows a problem;
* ``ledger``: the budget and what ``ctx.once`` keeps;
* ``recorder``: what goes into the journal, and rebuilding a run from it;
* ``clock``: time limits, and which activation has overrun;
* ``context``: the ``ctx`` a node is given.
"""
from __future__ import annotations

import asyncio
import inspect
import secrets
import threading
import time
from collections import deque
from typing import Any, Callable, Iterable, Mapping

from ..base import Change, Graph, GraphBuilder, GraphError, Invalid, Node, NodeError, Protocols, canonical, waited
from ..base.envelope import Envelope
from .aio import Signal, Threads, athrough, through, to_end
from .checks import Checks
from .clock import overrun, set_limits
from .context import RunContext
from .durable import DurableState, copy_body, payload_key
from .journal import CheckpointJournal, JournalBackend, pack, pack_body, unpack_body
from .ledger import Ledger, amounts
from .recorder import Act, Recorder, _envelope, restore, under
from .rework import Superseded
from .result import OverBudget, Result, RunError, WaitingForTasks
from .scheduler import Scheduler
from .stream import Stream
from .transport import Transport
from .recorder import SWITCHED
from .wiring import ACTIVE, INPUT, PAUSED, STOPPED, Feed, Rejection, Sent, State, bounded, wire

__all__ = ["Engine", "OverBudget", "Result", "RunError"]

SIBLINGS = 32       # how many other messages of the same activation a rejection names, and a port keeps to reject


class Engine:
    """One run of one graph. Build a new Engine to run again."""

    def __init__(self, graph: Graph, protocols: Protocols | None = None, *, workers: int = 0,
                 stopping: Callable[[], bool] | None = None, transport: Transport | None = None,
                 journal: JournalBackend | None = None, time_limit: float | None = None,
                 observe: Callable[[Mapping], None] | None = None, budget: Mapping[str, float] | None = None,
                 firm: bool = False, rebuild: Callable[[dict], Node] | None = None, max_nodes: int | None = None):
        if workers < 0:
            raise ValueError("workers cannot be negative")
        if time_limit is not None and not time_limit > 0:
            raise ValueError("time_limit must be a positive number of seconds")
        if protocols is None:
            protocols = graph.protocols
            if protocols is None:
                raise RunError("the graph was not compiled with protocols: give the engine the ones to run it with")
        if max_nodes is not None and (type(max_nodes) is not int or max_nodes < 1):
            raise ValueError("max_nodes must be a whole number of at least 1")
        self.graph, self.protocols, self.workers = graph, protocols, workers   # graph: as it stands now
        self._rebuild, self._max_nodes = rebuild, max_nodes
        self._observe = observe
        self._ledger = Ledger(budget)
        self._id: str | None = None         # names this run: the first part of every activation's key
        self._waiting_tasks: list[str] = []
        self._limit = time_limit
        self._firm = bool(firm)             # no node may give itself longer than time_limit
        self._parent: tuple | None = None   # (outer engine, the outer node's state, its activation's record)
        self._within: tuple = ()            # (node id, round) of each node this run is inside, outermost first
        self._over = False                # set once the engine has returned: a node still running is on its own
        self._alive = 0                   # worker threads that have not returned
        self._outer = stopping
        self._carry = transport.carry if transport is not None else None
        self._durable = DurableState() if isinstance(journal, CheckpointJournal) and journal.durable else None
        self._recorder = Recorder(journal, self._durable) if journal is not None else None
        if self._durable is not None:
            self._check_durable_graph(graph)
            if transport is not None:
                raise GraphError("durable SQLite journals do not support a custom transport")
        self._orphans: set[str] = set()   # after a resume: envelopes already dealt with whose sender must run again
        self._inside: dict[str, "Engine"] = {}   # node id -> the engine of the graph it runs inside itself now
        self._lock = Signal()
        self._sched = Scheduler()
        self._active = 0
        self._parked = 0                  # activations waiting at a checkpoint because their node is paused
        self._waiters: set[State] = set()  # activations waiting for a message (ctx.receive): they hold no worker
        self._retire = 0                  # worker threads to let go, lent while an activation waited
        self._threads: Threads | None = None
        self._error: RunError | None = None
        self._result = Result(outputs={name: [] for name in graph.outputs})
        self._checks = Checks(self._result.problems, observe is not None)
        self._limited: dict[str, None] = {}
        self._used = False
        wiring = self._wiring = wire(graph, protocols)
        self._states, self._feeds, self._inputs = wiring.states, wiring.feeds, wiring.inputs
        self._timed = set_limits(self._states.values(), time_limit, self._firm)
        self._asynchronous = any(state.node.asynchronous for state in self._states.values())

    @property
    def used(self) -> dict[str, float]:
        """What the nodes have reported using so far, added up."""
        with self._lock:
            return dict(self._ledger.meter)

    # ---- running ---------------------------------------------------------------------------------------

    def run(self, inputs: Mapping[str, Any] | None = None) -> Result:
        """Feed the graph's inputs, run until nothing is left to do, and return what reached its outputs."""
        self._begin()
        self._feed(inputs)
        return self._drive()

    def go(self, inputs: Mapping[str, Any] | None = None, *, fresh: bool = False) -> Result:
        """Run, or, when the journal already holds a run of this graph, take that run up. ``fresh`` throws
        an earlier run away."""
        return self.resume() if self._taken_up(fresh) else self.run(inputs)

    def resume(self, *, change: bool = False) -> Result:
        """Take up the run in the journal where it stopped. ``change=True`` allows this engine's graph to
        differ from the one the journal was written under."""
        self._begin()
        return self._drive(self._restore(change))

    @property
    def going(self) -> bool:
        """Whether a run of this engine is under way now (and so can be controlled)."""
        return bool(self._used) and not self._over

    def stop(self) -> None:
        """Ask the run to end: nothing new starts; running nodes see ``ctx.stopping()``."""
        with self._lock:
            self._sched.stopped = True
            self._lock.notify_all()

    # ---- the same, awaited ------------------------------------------------------------------------------

    async def arun(self, inputs: Mapping[str, Any] | None = None) -> Result:
        """``run``, for a caller that is a coroutine. Cancelling the awaiting task stops the run. When the
        graph has nodes that are coroutines, the run goes on in this event loop; otherwise in threads, and
        the loop stays free."""
        if not self._asynchronous:
            return await self._awaited(self.run, inputs)
        self._begin()
        self._feed(inputs)
        return await self._afinish()

    async def aresume(self, *, change: bool = False) -> Result:
        """``resume``, awaited."""
        if not self._asynchronous:
            return await self._awaited(lambda: self.resume(change=change))
        self._begin()
        return await self._afinish(self._restore(change))

    async def ago(self, inputs: Mapping[str, Any] | None = None, *, fresh: bool = False) -> Result:
        """``go``, awaited."""
        if not self._asynchronous:
            return await self._awaited(lambda: self.go(inputs, fresh=fresh))
        return await (self.aresume() if self._taken_up(fresh) else self.arun(inputs))

    async def _awaited(self, do: Callable, *args) -> Result:
        try:
            return await asyncio.to_thread(do, *args)
        except asyncio.CancelledError:
            self.stop()
            raise

    # ---- the life of a run -----------------------------------------------------------------------------

    def _begin(self) -> None:
        if self._used:
            raise RunError("this engine has already run; build a new one")
        self._used = True

    def _feed(self, inputs: Mapping[str, Any] | None) -> None:
        """Start a new run: put the graph's inputs in and queue what can run."""
        recorder = self._recorder
        if recorder is not None and (recorder.journal.has_records() if isinstance(recorder.journal, CheckpointJournal)
                                     else bool(recorder.read())):
            raise RunError("the journal already holds a run: resume it, or use a new journal")
        inputs = dict(inputs or {})
        unknown = inputs.keys() - self._inputs.keys()
        if unknown:
            raise RunError(f"the graph has no input {sorted(unknown)}; its inputs are {sorted(self._inputs)}")
        sched = self._sched
        with self._lock:
            sent = []
            for name, payload in inputs.items():
                feed = self._inputs[name]
                try:
                    feed.port.port.schema.check(payload)
                    if self._durable is not None:
                        payload = copy_body(payload)
                except (TypeError, ValueError) as exc:
                    raise RunError(f"graph input {name}: {exc}") from None
                envelope = sched.envelope(feed, payload, 0, ())
                sched.place(feed, envelope)
                sent.append(envelope)
            self._id = self._id or secrets.token_hex(8)
            if recorder is not None:
                try:
                    recorder.run(self._id, self.graph.digest, sent)
                except (TypeError, ValueError) as exc:
                    raise RunError(f"the graph's inputs cannot be journaled: {exc}") from None
            for state in self._states.values():
                sched.consider(state)

    def _taken_up(self, fresh: bool) -> bool:
        """For ``go``: whether the journal holds a run of this graph to take up. One that cannot be, or
        that ``fresh`` asks to forget, is thrown away."""
        recorder = self._recorder
        if recorder is not None:
            if self._durable is not None and not fresh:
                assert isinstance(recorder.journal, CheckpointJournal)
                return recorder.journal.has_records()
            records = recorder.read()
            if records:
                if not fresh and records[0].get("durable"):
                    raise RunError("this journal requires durable SQLite delivery")
                if not fresh and under(records) == self.graph.digest:
                    return True
                if self._durable is not None:
                    recorder.journal.discard()
                else:
                    recorder.journal.reset()
        return False

    @staticmethod
    def _check_durable_graph(graph: Graph) -> None:
        unsupported = [edge.id for edge in graph.edges if edge.protocol not in ("value", "artifact") or edge.challenges]
        if unsupported:
            raise GraphError(f"durable SQLite journals require value or artifact edges without challenges: {', '.join(unsupported)}")

    def _restore_durable(self, recorder: Recorder, change: bool) -> bool:
        if change:
            raise RunError("resume the original durable graph before changing it through ctx.change")
        assert isinstance(recorder.journal, CheckpointJournal)
        _, restored = DurableState.load(recorder.journal)
        start = restored.data["run"]
        if start is None:
            raise RunError("the journal is empty: there is nothing to resume")
        if start["graph"] != self.graph.digest:
            raise RunError("the journal was written under another durable graph")
        if self._id is not None and start["id"] != self._id:
            raise RunError("the durable child journal belongs to another activation")
        with self._lock:
            self._durable = recorder.durable = restored
            self._id = start["id"]
            restored.rebuild(self, SWITCHED)
            if not restored.data["ended"]:
                over = self._ledger.over()
                if over:
                    raise RunError(f"{over}: it goes on only under a larger budget")
                for state in self._states.values():
                    self._sched.consider(state)
        return bool(restored.data["ended"])

    def _restore(self, change: bool) -> bool:
        """Put the run in the journal back as it was. Returns whether that run had already ended."""
        recorder = self._recorder
        if recorder is None:
            raise RunError("resume needs a journal")
        if self._durable is not None:
            return self._restore_durable(recorder, change)
        records = recorder.read()
        if not records:
            raise RunError("the journal is empty: there is nothing to resume")
        if records[0].get("durable"):
            raise RunError("this journal requires durable SQLite delivery")
        was = under(records)
        assert was is not None
        changed = was != self.graph.digest
        if changed and not change:
            raise RunError(f"the journal was written under another graph ({was[:12]}); "
                           f"pass change=True to continue under this one ({self.graph.digest[:12]})")
        ended = records[-1]["kind"] == "end" and not changed
        with self._lock:
            run_id, self._orphans, dropped = restore(records, self._wiring, self._sched, self._ledger, self._result,
                                                     self._limited, self._remake, same_graph=not changed)
            self._id = self._id or run_id
            if not ended:
                over = self._ledger.over()
                if over:
                    raise RunError(f"{over}: it goes on only under a larger budget")
            if changed:
                recorder.change(was, self.graph.digest, dropped)
            if not ended:
                for state in self._states.values():
                    self._sched.consider(state)
        return ended

    def _stopping(self) -> bool:
        return self._sched.stopped or (self._outer is not None and bool(self._outer()))

    # ---- the switch on each node -------------------------------------------------------------------------

    def control(self, nodes: str | Iterable[str], action: str, *, team: bool = False) -> list[str]:
        """Set the switch of one node or several while the run goes on. ``action``:

        * ``pause``: it does not start again; one that is running stops at its next step (``ctx.checkpoint``)
          and waits there, holding its place;
        * ``resume``: back on; a paused node goes on from where it waited, a stopped one takes up what waits
          for it;
        * ``stop``: off; one that is running ends at its next step, and what it took is put back for later;
        * ``restart``: what it is doing is dropped as for ``stop``, what it holds is forgotten (``Node.reset``),
          and it starts again.

        ``team=True`` takes in every node the named ones manage, all the way down. A node of a graph that a
        node runs inside itself is named by path, ``"<node>/<id>"`` (and so on further in); whoever may switch
        that node may switch what is inside it. Returns the nodes whose switch changed. Every change is in the
        journal, and a resumed run keeps it."""
        return self._control(nodes, action, team, None)

    def _control(self, nodes: str | Iterable[str], action: str, team: bool, by: str | None) -> list[str]:
        if action not in SWITCHED:
            raise ValueError(f"no action {action!r}: one of {', '.join(SWITCHED)}")
        if self._durable is not None and action == "restart":
            raise NodeError("durable activations support pause, stop and resume")
        ids = [nodes] if isinstance(nodes, str) else list(nodes)
        inside = [id for id in ids if "/" in id]
        if inside:
            ids = [id for id in ids if "/" not in id]
            changed = self._control(ids, action, team, by) if ids else []
            return changed + [f"{head}/{done}" for head, inner, rest in self._insides(inside, by)
                              for done in inner._control(rest, action, team, None)]
        lock = self._lock
        with lock:
            if not self._used or self._over:
                raise RunError("only a run that is going can be controlled here; before or after, control it "
                               "through its run directory")
            graph = self.graph
            for id in ids:
                if id not in self._states:
                    raise RunError(f"no node {id!r}")
            if by is not None:
                under = set(graph.team(by))
                outside = [id for id in ids if id not in under]
                if outside:
                    raise NodeError(f"{by} does not manage {', '.join(outside)}")
            if team:
                ids = [*ids, *(member for id in ids for member in graph.team(id))]
            if self._durable is not None:
                changed = [id for id in dict.fromkeys(ids) if
                           (self._states[id].status == ACTIVE if action == "pause"
                            else self._states[id].status != SWITCHED[action])]
                if changed:
                    self._write_durable(self._states[changed[0]], {"kind": "control", "nodes": changed,
                                        "action": action, **({"by": by} if by else {})})
                    for id in changed:
                        self._switch(self._states[id], action)
            else:
                changed = [id for id in dict.fromkeys(ids) if self._switch(self._states[id], action)]
                if changed and self._recorder is not None:
                    self._recorder.control(changed, action, by)
            lock.notify_all()
        if changed and self._observe is not None:
            self._tell("control", nodes=changed, action=action, **({"by": by} if by else {}))
        return changed

    def _insides(self, paths: list[str], by: str | None) -> list[tuple]:
        """(node, the engine running inside it, the rest of the path) for each path, once ``by`` is found to
        manage the node (or is the operator)."""
        out = []
        with self._lock:
            for path in paths:
                head, rest = path.split("/", 1)
                if head not in self._states:
                    raise RunError(f"no node {head!r}")
                if by is not None and head not in self.graph.team(by):
                    raise NodeError(f"{by} does not manage {head}")
                inner = self._inside.get(head)
                if inner is None or inner._over or not inner._used:
                    raise RunError(f"{head} is not running a graph of its own now")
                out.append((head, inner, rest))
        return out

    def _team(self, by: str) -> dict:
        """The nodes ``by`` manages, all the way down, and how each stands: for ``ctx.team``."""
        with self._lock:
            graph = self.graph
            return {id: {"status": state.status, "running": state.running, "waiting": state.waiting,
                         "manager": graph.managers.get(id), "rounds": state.round}
                    for id in graph.team(by) for state in (self._states[id],)}

    def _switch(self, state: State, action: str) -> bool:
        """Set one node's switch. Called under the lock. Returns whether anything changed."""
        sched, was = self._sched, state.status
        if was == PAUSED and action != "pause" and state.running:
            state.since += time.monotonic() - state.paused_at    # time spent paused does not count against its limit
        if action == "pause":
            if was != ACTIVE:
                return False
            state.status = PAUSED
            state.paused_at = time.monotonic()
            sched.withdraw(state)
        elif action == "stop":
            if was == STOPPED:
                return False
            state.status = STOPPED
            sched.withdraw(state)
            if state.running:
                state.halt = "stop"         # what it was asked and did not answer is refused when it ends
            else:
                self._unheard(state, "the sender was stopped, and cannot answer")
        elif action == "resume":
            if was == ACTIVE:
                return False
            state.status = ACTIVE
            if not state.running:
                sched.consider(state)
        else:                                   # restart
            state.status = ACTIVE
            sched.withdraw(state)
            if state.running:
                state.halt = "restart"
            else:
                state.node.reset()
                sched.consider(state)
        return True

    def _checkpoint(self, state: State, ctx: RunContext) -> bool:
        """A step boundary in a running activation: wait here while the node is paused. False when the
        activation is to end now (the run is stopping, or the node was stopped or restarted)."""
        if state.status == PAUSED and not state.halt:
            lock, sched = self._lock, self._sched
            with lock:
                self._parked += 1
                lock.notify_all()
                try:
                    while state.status == PAUSED and not state.halt and not sched.stopped and not self._over:
                        if self._idle() or (self._outer is not None and self._outer()):
                            break
                        lock.wait(0.1)
                finally:
                    self._parked -= 1
        if state.halt or self._stopping():
            ctx._cut = True
            return False
        return True

    async def _acheckpoint(self, state: State, ctx: RunContext) -> bool:
        """``_checkpoint``, awaited: a paused coroutine waits without holding the loop."""
        if state.status == PAUSED and not state.halt:
            with self._lock:
                self._parked += 1
                self._lock.notify_all()
            try:
                while state.status == PAUSED and not state.halt and not self._sched.stopped and not self._over:
                    with self._lock:
                        if self._idle():
                            break
                    if self._outer is not None and self._outer():
                        break
                    await asyncio.sleep(0.05)
            finally:
                with self._lock:
                    self._parked -= 1
        if state.halt or self._stopping():
            ctx._cut = True
            return False
        return True

    def _idle(self) -> bool:
        """Every activation under way is paused, and nothing else can start: the run ends here, to be taken up
        once someone switches them on again. Called under the lock."""
        if (self._parked + len(self._waiters) >= self._active and not self._sched.ready
                and not any(self._mail(state) or self._stirred(state) for state in self._waiters)):
            for state in self._states.values():
                if state.running and state.status == PAUSED and not state.halt:
                    state.halt = "park"
            return True
        return False

    # ---- progress saved in the middle of an activation --------------------------------------------------

    def _commit(self, state: State, ctx: RunContext) -> bool:
        """``ctx.commit``: put what the activation has done so far on record (see ``Context.commit``)."""
        act, node = ctx._act, state.node
        with self._lock:
            if not state.running or ctx._state is not state:
                raise NodeError(f"{node.id} can save progress only in an activation of its own")
            if ctx._cut or self._over:
                return False
            self._check_current(state)
            if self._is_rework(state):
                raise NodeError("a rework activation cannot save intermediate progress")
            if state.replay:
                raise NodeError("durable replay still has committed operations to reproduce")
            ctx._saved = True
            if act is None or self._recorder is None:
                state.holding = []
                return False
            self._snap(node, act)
            try:
                self._recorder.act(act, open=True, children=state.inner.values())
            except Exception as exc:   # noqa: BLE001 - something it sent or holds is not plain data
                if self._durable is not None:
                    self._abort_durable(state, exc)
                raise NodeError(f"its progress cannot be journaled: {exc}") from None
            ctx._act = Act(node.id, act.round, ())
            state.holding = []          # what it took so far is used up for good: a cut no longer gives it back
        if self._observe is not None:
            self._tell("saved", node=node.id, round=state.round)
        return True

    # ---- a message in the middle of an activation ------------------------------------------------------

    @staticmethod
    def _mail(state: State, ports: frozenset | None = None) -> Feed | None:
        """The feed holding the next message for a running node, in the order they came; None when there is none."""
        if state.status != ACTIVE:
            return None                         # paused: it takes nothing until it is switched on again
        for feed in state.late:
            if feed.box and not feed.gone and (ports is None or feed.port.port.name in ports):
                return feed
        for name, port in state.ports.items():
            if ports is None or name in ports:
                for feed in (*port.forward, *port.back):
                    if feed.box and not feed.gone:
                        return feed
        return None

    def _taken(self, state: State, ctx: RunContext, feed: Feed) -> dict:
        """Take the next message from ``feed`` for a running activation. Under the lock."""
        envelope = self._sched.pop(feed)
        try:
            state.late.remove(feed)
        except ValueError:
            pass
        state.holding.append((feed, envelope))
        bounded(state.read_log, envelope.id, None)      # read in the middle: a correction of it is told
        port = feed.port.port
        took = state.took_on.setdefault(port.name, [])
        took.append((feed, envelope))
        if len(took) > SIBLINGS:                # a long activation keeps the newest ones it could still reject
            del took[0]
        if envelope.replaces:
            state.replaced[port.name] = envelope.replaces
        else:
            state.replaced.pop(port.name, None)
        ctx._cause = (*ctx._cause, envelope.id)
        if ctx._act is not None:
            ctx._act.took = (*ctx._act.took, envelope.id)
        value = self._checks.arrived(state, feed, envelope.body)
        return {port.name: [value] if port.many else value}

    def _hear(self, state: State, ctx: RunContext, ports: frozenset | None, until: float | None) -> tuple:
        """One look, under the lock: ("got", message), ("none", None) when nothing can come or the time is up
        or the node is to end, or ("wait", seconds)."""
        if len(state.reviews.pending) > state.reviews.shown:      # something it sent was rejected: it is told at once
            state.reviews.shown = len(state.reviews.pending)
            return "got", {}
        if len(state.failures) > state.failures_shown:      # a node it manages failed: it is told at once
            state.failures_shown = len(state.failures)
            return "got", {}
        feed = self._mail(state, ports)
        if feed is not None:
            return "got", self._taken(state, ctx, feed)
        if state.halt or self._stopping() or self._over:
            ctx._cut = ctx._cut or bool(state.halt) or self._stopping()
            return "none", None
        if self._idle():
            if self._unfinished():          # nodes switched off still hold work: end here, to be taken up later
                state.halt = state.halt or "park"
                ctx._cut = True
            return "none", None
        left = None if until is None else until - time.monotonic()
        if left is not None and left <= 0:
            return "none", None
        return "wait", 0.1 if left is None else min(left, 0.1)

    def _receive(self, state: State, ctx: RunContext, ports, timeout: float | None) -> dict | None:
        """``ctx.receive``: wait for the next message to this running activation (see ``Context.receive``)."""
        if self._durable is not None:
            raise NodeError("durable delivery binds inputs at activation start")
        ports = self._ports(state, ports)
        until = None if timeout is None else time.monotonic() + timeout
        lock = self._lock
        with lock:
            self._own(state, ctx)
            what, got = self._hear(state, ctx, ports, until)
            if what == "wait":
                began = time.monotonic()
                self._lend(state)
                lock.notify_all()
                try:
                    while what == "wait":
                        lock.wait(got)
                        what, got = self._hear(state, ctx, ports, until)
                finally:
                    self._take_back(state)
                    state.since += time.monotonic() - began       # waiting does not count against its time limit
        return self._heard(state, got)

    async def _areceive(self, state: State, ctx: RunContext, ports, timeout: float | None) -> dict | None:
        """``_receive``, awaited: the coroutine waits without holding the loop."""
        if self._durable is not None:
            raise NodeError("durable delivery binds inputs at activation start")
        ports = self._ports(state, ports)
        until = None if timeout is None else time.monotonic() + timeout
        lock = self._lock
        with lock:
            self._own(state, ctx)
            what, got = self._hear(state, ctx, ports, until)
            if what == "wait":
                began = time.monotonic()
                self._lend(state)
                lock.notify_all()
        if what == "wait":
            try:
                while what == "wait":
                    await asyncio.sleep(min(got, 0.05))
                    with lock:
                        what, got = self._hear(state, ctx, ports, until)
            finally:
                with lock:
                    self._take_back(state)
                    state.since += time.monotonic() - began
        return self._heard(state, got)

    @staticmethod
    def _own(state: State, ctx: RunContext) -> None:
        if not state.running or ctx._state is not state or state.waiting:
            raise NodeError(f"{state.node.id} can receive only in an activation of its own, one message at a time")

    def _heard(self, state: State, got: dict | None) -> dict | None:
        if got is not None and self._observe is not None:
            self._tell("received", node=state.node.id, round=state.round, ports=list(got))
        return got

    @staticmethod
    def _ports(state: State, ports) -> frozenset | None:
        if ports is None:
            return None
        names = frozenset([ports] if isinstance(ports, str) else ports)
        unknown = names - set(state.ports)
        if unknown:
            raise NodeError(f"{state.node.id} has no input port {', '.join(sorted(unknown))}; its inputs are {sorted(state.ports)}")
        return names

    # ---- rejections: a receiver says what came is not what it expected --------------------------------

    @staticmethod
    def _stirred(state: State) -> bool:
        """A waiting activation has something to wake for besides a message. Under the lock."""
        awaiting = state.reviews.awaiting
        return (len(state.reviews.pending) > state.reviews.shown or len(state.failures) > state.failures_shown
                or (awaiting is not None and awaiting.answer is not None))

    @staticmethod
    def _chosen(state: State, port: str, which: str | None) -> tuple | None:
        """Of what this activation took on ``port``, the one ``which`` names (the id of the envelope, the edge
        or the node that sent it); the last one when it names none."""
        took = state.took_on.get(port)
        if not took:
            return None
        if which is None:
            return took[-1]
        return next((got for got in reversed(took) if which in (got[1].id, got[0].id, got[0].source.node.id
                                                                if got[0].source is not None else None)), None)

    def _challengeable(self, state: State, port: str, which: str | None = None) -> bool:
        self._ports(state, port)
        with self._lock:
            got = self._chosen(state, port, which)
            if got is None:
                return False
            feed = got[0]
            return feed.challenges is not None and feed.rejected < feed.challenges and not feed.gone

    def _taken_on(self, state: State, port: str) -> list:
        """``ctx.taken``: what this activation took on ``port``, in order: its id and who sent it."""
        self._ports(state, port)
        with self._lock:
            return [{"id": envelope.id, "from": feed.source.node.id if feed.source is not None else None,
                     "edge": feed.id} for feed, envelope in state.took_on.get(port, ())]

    def _rejection(self, state: State, ctx: RunContext, port: str, reason: str,
                   which: str | None = None) -> Rejection | dict:
        """Send a rejection to the sender of what was taken on ``port`` (the one ``which`` names). Under the
        lock. A mapping when it cannot be sent: that is the answer."""
        self._own(state, ctx)
        self._ports(state, port)
        got = self._chosen(state, port, which)
        if got is None:
            if which is not None and state.took_on.get(port):
                raise NodeError(f"{state.node.id} took nothing on {port!r} in this activation that {which!r} names")
            raise NodeError(f"{state.node.id} has taken nothing on {port!r} in this activation that it could reject")
        feed, envelope = got
        if feed.challenges is None:
            raise NodeError(f"{state.node.id}.{port}: the edge {feed.id} takes no rejections (give it challenges)")
        sender = feed.source
        if feed.rejected >= feed.challenges:
            why = f"the edge {feed.id} has taken all the rejections it may ({feed.challenges})"
            self._checks.record(state, port, f"rejected: {reason}; {why}", "unanswered", ctx._act, state.round)
            return {"answer": "refused", "why": why}
        if feed.gone or sender is None or sender.status == STOPPED or self._states.get(sender.node.id) is not sender:
            return {"answer": "refused", "why": "the sender is stopped or gone, and cannot answer"}
        assert sender is not None
        feed.rejected += 1
        if ctx._act is not None:
            ctx._act.rejects.append(feed.id)
        message = next((name for name, sent in reversed(sender.sent_log.items())
                        if any(id == envelope.id for _, id in sent.envelopes)), None)
        content = self._checks.arrived(state, feed, envelope.body)
        sent: dict[str, list] = {}
        if message is not None:                 # what the same activation sent: it may be wrong too
            round = sender.sent_log[message].round
            others = [name for name, other in reversed(sender.sent_log.items())
                      if other.round == round and name != message][:SIBLINGS]
            for name in reversed(others):
                sent.setdefault(sender.sent_log[name].port, []).append(name)
        rejection = Rejection(f"{feed.id}!{feed.rejected}", feed, envelope.id, feed.out, message, content, str(reason),
                              state.node.id, sent)
        if self._recorder is not None:
            self._recorder.review("challenged", rejection)
        sender.reviews.pending.append(rejection)
        state.reviews.awaiting = rejection
        self._sched.consider(sender)
        self._lock.notify_all()
        return rejection

    def _settled(self, state: State, ctx: RunContext, rejection: Rejection) -> dict | None:
        """The answer to the rejection this activation waits on, once there is one. Under the lock."""
        if rejection.answer is None:
            if state.halt or self._stopping() or self._over:
                ctx._cut = ctx._cut or bool(state.halt) or self._stopping()
                self._withdraw(rejection, "the run is ending, or this node is to end")
            elif self._idle():
                if self._unfinished():
                    state.halt = state.halt or "park"
                    ctx._cut = True
                self._withdraw(rejection, "no answer can come: the sender has ended or waits too")
            else:
                return None
        state.reviews.awaiting = None
        if self._recorder is not None:
            self._recorder.review("verdict", rejection)
        return rejection.verdict()

    @staticmethod
    def _withdraw(rejection: Rejection, why: str) -> None:
        rejection.settle("refused", why)
        sender = rejection.feed.source
        if sender is not None:
            try:
                sender.reviews.pending.remove(rejection)
            except ValueError:
                sender.reviews.open.pop(rejection.id, None)

    def _unheard(self, state: State, why: str) -> None:
        """``state`` will not answer the rejections of what it sent: each is refused now. Under the lock."""
        waiting = [*state.reviews.pending, *state.reviews.open.values()]
        if not waiting:
            return
        for rejection in waiting:
            if rejection.answer is None:
                rejection.settle("refused", why)
        state.reviews.pending.clear()
        state.reviews.open.clear()
        self._lock.notify_all()

    def _reject(self, state: State, ctx: RunContext, port: str, reason: str, which: str | None = None) -> dict:
        """``ctx.reject``: see ``Context.reject``."""
        lock = self._lock
        with lock:
            rejection = self._rejection(state, ctx, port, reason, which)
            if isinstance(rejection, dict):
                return rejection
            got = self._settled(state, ctx, rejection)
            if got is None:
                began = time.monotonic()
                self._lend(state)
                lock.notify_all()
                try:
                    while got is None:
                        lock.wait(0.1)
                        got = self._settled(state, ctx, rejection)
                finally:
                    self._take_back(state)
                    state.since += time.monotonic() - began
        return self._answered(state, port, rejection, got)

    async def _areject(self, state: State, ctx: RunContext, port: str, reason: str,
                       which: str | None = None) -> dict:
        """``_reject``, awaited."""
        lock = self._lock
        with lock:
            rejection = self._rejection(state, ctx, port, reason, which)
            if isinstance(rejection, dict):
                return rejection
            got = self._settled(state, ctx, rejection)
            if got is None:
                began = time.monotonic()
                self._lend(state)
                lock.notify_all()
        if got is None:
            try:
                while got is None:
                    await asyncio.sleep(0.02)
                    with lock:
                        got = self._settled(state, ctx, rejection)
            finally:
                with lock:
                    self._take_back(state)
                    state.since += time.monotonic() - began
        return self._answered(state, port, rejection, got)

    def _answered(self, state: State, port: str, rejection: Rejection, got: dict) -> dict:
        if self._observe is not None:
            self._tell("answered", node=state.node.id, round=state.round, port=port, rejection=rejection.id,
                       answer=got["answer"])
        return got

    def _failures(self, state: State, ctx: RunContext) -> list:
        """``ctx.failures``: take up the failures of nodes this one manages."""
        with self._lock:
            if not state.running or ctx._state is not state:
                raise NodeError(f"{state.node.id} can take up failures only in an activation of its own")
            taken, state.failures = state.failures, []
            state.failures_shown = 0
            return taken

    def _reported(self, state: State, failure: BaseException) -> bool:
        """``state``'s activation failed: when its manager oversees it and is running, the failure goes to the
        manager and the run goes on, with ``state`` stopped. Under the lock."""
        boss = self._states.get(self.graph.managers.get(state.node.id, ""))
        if (boss is None or not boss.node.oversees or not boss.running or boss.status == STOPPED or boss.halt
                or self._sched.stopped or self._error is not None):
            return False
        sched = self._sched
        self._end_streams(state)
        state.running = False
        self._active -= 1
        state.halt = False
        sched.give_back(state)                  # what it took waits for it, should it be restarted
        state.status = STOPPED
        sched.withdraw(state)
        self._unheard(state, "the sender failed, and cannot answer")
        error = str(failure)
        self._result.problems.append({"node": state.node.id, "round": state.round, "problem": error,
                                      "handled": "reported", "to": boss.node.id})
        boss.failures.append({"node": state.node.id, "round": state.round, "error": error})
        self._lock.notify_all()
        return True

    def _rejections(self, state: State, ctx: RunContext) -> list:
        """``ctx.rejections``: take up the rejections of what this node sent."""
        with self._lock:
            if not state.running or ctx._state is not state:
                raise NodeError(f"{state.node.id} can take up rejections only in an activation of its own")
            return state.reviews.take()

    def _explain(self, state: State, id: str, text: str) -> None:
        """``ctx.explain``: what was rejected stands, for this reason."""
        with self._lock:
            rejection = state.reviews.open.pop(str(id), None)
            if rejection is None:
                raise NodeError(f"{state.node.id} has no rejection {id!r} to answer")
            rejection.settle("explained", str(text))
            self._lock.notify_all()

    def _answer(self, state: State, ctx: RunContext) -> None:
        """An activation that answers a rejection no activation of the node took up: ``Node.challenged``."""
        rejection = state.reviews.answering
        assert rejection is not None
        with self._lock:
            if rejection.answer is not None:
                return
            state.reviews.open[rejection.id] = rejection
        answer = waited(state.node.challenged(ctx, rejection.shown()))
        self._unanswered(state, rejection, answer)

    async def _aanswer(self, state: State, ctx: RunContext) -> None:
        rejection = state.reviews.answering
        assert rejection is not None
        with self._lock:
            if rejection.answer is not None:
                return
            state.reviews.open[rejection.id] = rejection
        answer = state.node.challenged(ctx, rejection.shown())
        if inspect.isawaitable(answer):
            answer = await answer
        self._unanswered(state, rejection, answer)

    def _unanswered(self, state: State, rejection: Rejection, answer: Any) -> None:
        with self._lock:
            state.reviews.open.pop(rejection.id, None)
            if state.status == STOPPED:         # stopped while it answered: what it returns is not its answer
                settled = rejection.settle("refused", "the sender was stopped, and cannot answer")
            else:
                settled = rejection.settle("explained", str(answer) if answer is not None else "no reason was given")
            if settled:
                self._lock.notify_all()

    def _unfinished(self) -> dict[str, str]:
        """Nodes whose switch is off and that still have something waiting for them."""
        held = {}
        for id, state in self._states.items():
            if state.status != ACTIVE and (state.running or state.bound or state.rework or any(
                    feed.box for port in state.ports.values() for feed in (*port.forward, *port.back))):
                held[id] = state.status
        return held

    def _drive(self, ended: bool = False) -> Result:
        """Run to the end in this thread: on an event loop when there are nodes to await, else in threads."""
        if self._asynchronous and not ended:
            return to_end(lambda: self._afinish())
        return self._finish(ended)

    def _finish(self, ended: bool = False) -> Result:
        try:
            if ended:
                pass
            elif self._timed:
                self._watch()
            else:
                self._alive = self.workers or 1
                for i in range(self.workers):
                    threading.Thread(target=self._worker, name=f"engine-{i}", daemon=True).start()
                if not self.workers:
                    self._worker()            # one at a time, in this thread
                with self._lock:
                    while self._alive:        # the workers, and any lent while an activation waited
                        self._lock.wait()
        finally:
            with self._lock:
                self._over = True
            for state, closing in self._close():
                try:
                    waited(closing)
                except Exception as exc:   # noqa: BLE001
                    self._close_failed(state, exc)
            self._wind_down(ended)
        return self._conclude()

    async def _afinish(self, ended: bool = False) -> Result:
        try:
            if not ended:
                await self._loop()
        finally:
            with self._lock:
                self._over = True
            for state, closing in self._close():
                try:
                    await closing
                except Exception as exc:   # noqa: BLE001
                    self._close_failed(state, exc)
            self._wind_down(ended)
        return self._conclude()

    def _wind_down(self, ended: bool) -> None:
        recorder = self._recorder
        if recorder is not None:
            if not ended and not self._sched.stopped and self._error is None and not self._unfinished():
                recorder.end()
            if not ended:
                recorder.checkpoint()
            recorder.close()

    def _conclude(self) -> Result:
        result = self._result
        if self._parent is not None and result.problems:
            outer, state, act = self._parent
            outer._adopt(state, act, result.problems)
        if self._error:
            self._flush()
            self._error.problems = result.problems
            self._error.used = dict(self._ledger.meter)
            self._tell("ended", complete=False, error=str(self._error))
            cause = self._error.__cause__
            if cause is not None and not isinstance(cause, Exception):
                raise cause                 # an interrupt or an exit request passes through as itself
            raise self._error
        if self._waiting_tasks:
            raise WaitingForTasks(self._waiting_tasks)
        result.envelopes = self._sched.envelopes
        result.rounds = {id: state.round for id, state in self._states.items() if state.round}
        result.limited = tuple(self._limited)
        result.held = {id: state.status for id, state in self._states.items() if state.status != ACTIVE}
        result.complete = not self._sched.stopped and not self._unfinished()
        result.used = dict(self._ledger.meter)
        self._flush()
        self._tell("ended", complete=result.complete)
        if self._error:                     # the observer failed on the last thing it was told
            raise self._error
        return result

    def _watch(self) -> None:
        """Run the workers in threads of their own and watch the clock for them. The order of activations is
        the same as without a limit: with no workers asked for, one thread does everything."""
        lock = self._lock
        threads = [threading.Thread(target=self._worker, name=f"engine-{i}", daemon=True) for i in range(self.workers or 1)]
        self._alive = len(threads)
        for thread in threads:
            thread.start()
        states = self._states.values()
        with lock:
            while self._alive:
                late, wait = overrun(states, time.monotonic())
                if late is not None:
                    self._late(late)
                    deadline = time.monotonic() + 1.0    # idle workers leave at once; a node still running is left
                    while (self._alive > sum(1 for s in states if s.running)
                           and time.monotonic() < deadline):
                        lock.wait(0.01)
                    return
                lock.wait(wait)

    def _worker(self) -> None:
        try:
            self._work()
        finally:
            with self._lock:
                self._alive -= 1
                self._lock.notify_all()

    def _lend(self, state: State) -> None:
        """An activation starts to wait for a message: what it holds of the workers goes to another. Called
        under the lock."""
        self._waiters.add(state)
        state.waiting = True
        if state.node.asynchronous:
            return                          # a coroutine waits on the loop and holds no thread
        if self._threads is not None:
            self._threads.size += 1         # on the event loop: one more thread for the nodes that are not coroutines
        elif self._alive:
            self._alive += 1
            threading.Thread(target=self._worker, name="engine-lent", daemon=True).start()

    def _take_back(self, state: State) -> None:
        """The waiting is over: one worker too many now, which leaves when it is next free. Under the lock."""
        self._waiters.discard(state)
        state.waiting = False
        if state.node.asynchronous:
            return
        if self._threads is not None:
            self._threads.size -= 1
        elif self._alive:
            self._retire += 1
        self._lock.notify_all()

    def _late(self, late: State) -> None:
        """An activation overran its time: the run fails, naming it. Called under the lock."""
        if self._error is None:
            self._error = RunError(f"node {late.node.id} ran longer than {late.time_limit:g} seconds "
                                   f"in round {late.round}", late.node.id)
        self._sched.stopped = True
        self._lock.notify_all()

    def _work(self) -> None:
        """One worker thread: take ready nodes and run them, until nothing is left."""
        lock, sched = self._lock, self._sched
        while True:
            with lock:
                if self._retire:
                    self._retire -= 1           # a worker lent to a waiting activation goes
                    return
                if self._outer is not None and not sched.stopped and self._outer():
                    sched.stopped = True
                while not sched.ready and self._active and not sched.stopped and not self._retire:
                    lock.wait()
                if self._retire:
                    continue
                if sched.stopped or not sched.ready:
                    lock.notify_all()
                    return
                picked = self._pick()
            if picked is not None and not self._whole(picked):
                return

    async def _loop(self) -> None:
        """The run on this event loop: nodes that are coroutines are awaited here, the others go to threads.
        At most ``workers`` activations (one, when that is 0) are under way at once, in the same order as
        in threads."""
        loop = asyncio.get_running_loop()
        lock, sched, states = self._lock, self._sched, self._states.values()
        wake = asyncio.Event()

        here = threading.get_ident()

        def hook() -> None:
            if threading.get_ident() == here:
                wake.set()
                return
            try:
                loop.call_soon_threadsafe(wake.set)
            except RuntimeError:
                pass                     # the loop has closed
        threads = self._threads = Threads(self.workers or 1, loop)
        running: dict[asyncio.Task, State] = {}
        limit = self.workers or 1

        def done(task: asyncio.Task) -> None:
            running.pop(task, None)
            wake.set()
        lock.hook = hook
        try:
            while True:
                wake.clear()
                with lock:
                    if self._outer is not None and not sched.stopped and self._outer():
                        sched.stopped = True
                    while sched.ready and not sched.stopped and len(running) - len(self._waiters) < limit:
                        picked = self._pick()
                        if picked is not None:
                            task = loop.create_task(self._one(picked, threads))
                            running[task] = picked[0]
                            task.add_done_callback(done)
                    if not running:
                        return
                    late, wait = overrun(states, time.monotonic()) if self._timed else (None, None)
                    if late is not None:
                        self._late(late)
                if late is not None:
                    await self._cancel(running)
                    return
                if self._outer is not None:
                    wait = 0.05 if wait is None else min(wait, 0.05)
                if wait is None:
                    await wake.wait()
                else:
                    try:
                        await asyncio.wait_for(wake.wait(), wait)
                    except asyncio.TimeoutError:
                        pass
        except asyncio.CancelledError:
            self.stop()                    # the one awaiting the run gave up on it
            await self._cancel(running)
            raise
        finally:
            lock.hook = None
            threads.close()
            self._threads = None

    async def _cancel(self, running: dict) -> None:
        """The run is over early: activations that are coroutines are cancelled, after a moment to see
        ``ctx.stopping()`` for themselves. Those in threads cannot be, and are left."""
        awaited = [task for task, state in running.items() if state.node.asynchronous]
        if awaited:
            await asyncio.wait(awaited, timeout=0.2)
            for task in awaited:
                task.cancel()
            await asyncio.wait(awaited, timeout=1.0)

    async def _one(self, picked: tuple, threads: Threads) -> None:
        state = picked[0]
        if not state.node.asynchronous:
            await threads.run(self._whole, picked)       # in a thread, from start to end, as without a loop
            return
        _, taken, cause, took = picked
        ctx, act = self._started(state, cause, took)
        failure = None
        try:
            await self._aactivate(state, ctx, act, taken, cause)
        except BaseException as exc:   # noqa: BLE001 - whatever a node raises ends the run; cancelled, it was cut off
            failure = exc
        self._ended(state, ctx, act, failure)

    def _whole(self, picked: tuple) -> bool:
        """One activation of a node that is not a coroutine, from start to end. Returns False when the engine
        had already returned."""
        state, taken, cause, took = picked
        ctx, act = self._started(state, cause, took)
        failure = None
        try:
            self._activate(state, ctx, act, taken, cause)
        except BaseException as exc:   # noqa: BLE001 - whatever a node raises ends the run, an interrupt included
            failure = exc
        return self._ended(state, ctx, act, failure)

    # ---- one activation --------------------------------------------------------------------------------

    def _pick(self) -> tuple | None:
        """Take the next ready node and what it takes. Called under the lock. None when what it was to take
        could not be read: it has failed."""
        sched, checks = self._sched, self._checks
        state = sched.ready.popleft()
        durable, node = self._durable, state.node.id
        pending = durable.pending.get(node) if durable is not None else None
        if pending is not None:
            state.round = pending["round"] - 1
        state.begin()
        if self._timed:
            state.since = time.monotonic()
            self._lock.notify_all()          # the watcher must learn that a clock has started
        self._active += 1
        try:
            def read(feed, body):
                return checks.arrived(state, feed, copy_body(body) if durable is not None else body)

            binding = pending
            if pending is not None:
                state.resuming, state.resumed = False, pending["resumed"]
                state.replay = deque(pending["ops"])
            if pending is None and durable is not None and state.rework:
                redo = next(iter(durable.rework.waiting[node]))
                reading = durable.rework.readings[node][redo]
                binding = {"redo": redo, "took": reading["took"], "redo_outputs": reading["emits"]}
            if durable is not None and binding is not None and "redo" in binding:
                took = cause = tuple(binding["took"])
                envelopes = []
                for id in took:
                    packed = durable.rework.deliveries[id]
                    feed = self._feeds[packed[0]]
                    envelopes.append((feed, _envelope(feed, packed)))
                taken = sched.retake(state, envelopes, read)
                state.redoing = {port: deque(tokens) for port, tokens in binding["redo_outputs"].items()}
            else:
                bound: list[Feed] | None = None
                if pending is not None:
                    bound = []
                    for id in pending["took"]:
                        feed = self._feeds[id.rsplit("#", 1)[0]]
                        if not feed.box or feed.box[0].id != id or feed.port.state is not state:
                            raise NodeError(f"cannot recover bound input {id!r}")
                        bound.append(feed)
                taken, cause, took = sched.take(state, read, bound=bound)
            if durable is not None and pending is None:
                assert self._recorder is not None
                self._recorder.write({"kind": "bind", "node": node, "round": state.round, "took": list(took),
                                      **(binding or {})})
                state.bound = True
        except Exception as exc:   # noqa: BLE001 - a protocol's decode failed
            self._fail(state, exc)
            return None
        if pending is None:
            self._result.activations += 1
        return state, taken, cause, took

    def _started(self, state: State, cause: tuple, took: tuple) -> tuple[RunContext, Act | None]:
        act = Act(state.node.id, state.round, took) if self._recorder is not None else None
        ctx = RunContext(self, state, cause, act)
        ctx.resumed = state.resumed
        if self._observe is not None:
            self._flush()
            self._tell("started", node=state.node.id, round=state.round, took=list(took))
        return ctx, act

    def _activate(self, state: State, ctx: RunContext, act: Act | None, taken: dict, cause: tuple) -> None:
        if self._durable is not None:
            with self._lock:
                self._check_current(state)
        node = state.node
        if not state.opened:
            state.opened = True
            waited(node.open(ctx))
        if state.reviews.answering is not None:
            self._answer(state, ctx)
            if act is not None:
                self._snap(node, ctx._act)
            return
        while True:
            try:
                returned = node.run(ctx, taken)
                if returned is not None:
                    if type(returned) is not dict:
                        returned = waited(returned)      # a coroutine, when the node was not known to be one
                    if not ctx._cut:                     # told to end: what it returns is not its answer
                        self._returned(state, returned, ctx._cause, ctx._act)
                break
            except Invalid as exc:
                if not self._again(exc, state, taken):
                    raise
                ctx.problems.append(str(exc))
        if act is not None:
            self._snap(node, ctx._act)

    async def _aactivate(self, state: State, ctx: RunContext, act: Act | None, taken: dict, cause: tuple) -> None:
        if self._durable is not None:
            with self._lock:
                self._check_current(state)
        node = state.node
        if not state.opened:
            state.opened = True
            opened = node.open(ctx)
            if inspect.isawaitable(opened):
                await opened
        if state.reviews.answering is not None:
            await self._aanswer(state, ctx)
            self._snap(node, ctx._act)
            return
        while True:
            try:
                returned = node.run(ctx, taken)
                if inspect.isawaitable(returned):
                    returned = await returned
                if not ctx._cut:
                    self._returned(state, returned, ctx._cause, ctx._act)
                break
            except Invalid as exc:
                if not self._again(exc, state, taken):
                    raise
                ctx.problems.append(str(exc))
        self._snap(node, ctx._act)

    def _returned(self, state: State, returned: Any, cause: tuple, act: Act | None) -> None:
        """Send on what ``run`` returned."""
        if returned is not None:
            if not isinstance(returned, Mapping):
                raise NodeError(f"run must return a mapping of output port to payload, or None; got {type(returned).__name__}")
            for port, payload in returned.items():
                self._emit(state, port, payload, state.round, cause, act)

    @staticmethod
    def _again(exc: Invalid, state: State, taken: dict) -> bool:
        """The node did not put right what it sent: whether to run it again, which is so if that repeats nothing."""
        return exc.again and not state.delivered and not any(isinstance(v, Stream) for v in taken.values())

    @staticmethod
    def _snap(node: Node, act: Act | None) -> None:
        if act is not None:
            snapshot = node.snapshot()
            if snapshot:
                act.snaps[node.id] = dict(snapshot)

    def _ended(self, state: State, ctx: RunContext, act: Act | None, failure: BaseException | None) -> bool:
        """An activation is over: record it, or what went wrong, and queue what can run next. Returns False
        when the engine has already returned and what the activation did is thrown away."""
        lock, sched, node = self._lock, self._sched, state.node
        act = ctx._act                  # the record it is writing now: a new one after each commit
        if self._durable is not None:
            with lock:
                if not self._over and self._error is None and not self._inputs_current(state):
                    return self._discard_stale(state, act)
        if failure is None and self._durable is not None and self._error is not None and self._error.node == node.id:
            failure = self._error
        if state.reviews.open:                  # rejections it took up and did not answer: Node.challenged answers them
            with lock:
                state.reviews.defer_open()
        if state.reviews.pending and state.status == STOPPED:
            with lock:
                self._unheard(state, "the sender was stopped, and cannot answer")
        if isinstance(failure, WaitingForTasks) and self._durable is not None:
            with lock:
                if self._over:
                    return False
                self._waiting_tasks.extend(failure.tasks)
                state.status = PAUSED  # this drive only: the durable binding is replayed when a command arrives
                self._release(state)
            return True
        if failure is not None:
            with lock:
                if self._over:
                    return False
                reported = self._reported(state, failure)
                if not reported:
                    self._fail(state, failure)
            if self._observe is not None:
                self._tell("failed", node=node.id, round=state.round, error=str(failure),
                           **({"to": self.graph.managers[node.id]} if reported else {}))
            return True
        if state.failures:              # failures of its members it never took up: the run fails as it would have
            with lock:
                first = state.failures[0]
                state.failures = []
                if self._error is None:
                    self._error = RunError(f"node {first['node']} failed in round {first['round']}: {first['error']} "
                                           f"(reported to {node.id}, which did not take it up)", first["node"])
                sched.stopped = True
                lock.notify_all()
        if ctx._cut:
            # it was told to stop and may have left its work half done: it is not on record as having run,
            # so it runs again when the run is taken up
            with lock:
                if self._over:
                    return False
                self._end_streams(state)
                state.running = False
                self._active -= 1
                halt, state.halt = state.halt, False
                if halt and not sched.stopped:
                    # only this node was stopped or restarted: what it took goes back, the run goes on
                    if halt == "restart" and ctx._saved:     # it starts again from what it first had
                        state.holding = [*state.opening, *state.holding]
                    sched.give_back(state)
                    if state.redoing is not None and state.reading is not None and halt != "restart":
                        state.reading.queued = True         # what it was running again, it runs again later
                        state.redo.appendleft(state.reading)
                    if halt == "restart":
                        state.node.reset()
                    elif ctx._saved:
                        state.resuming = True       # switched on again, it goes on from what it saved
                    if halt != "park" or state.status != ACTIVE:
                        sched.consider(state)
                    # parked while switched on: it was waiting on nodes that are paused, and the run is ending to
                    # be taken up later. Started again now it would only wait, be parked and start again, for ever;
                    # it starts again when something reaches it, or when the run is taken up.
                else:
                    sched.stopped = True
                lock.notify_all()
            if self._observe is not None:
                self._tell("stopped", node=node.id, round=state.round)
            return True
        if self._observe is not None:
            self._tell("finished", node=node.id, round=state.round)
        with lock:
            if self._over:             # it overran and the engine has gone: what it did is thrown away
                return False
            if self._durable is not None and not self._inputs_current(state):
                return self._discard_stale(state, act)
            self._end_streams(state)
            if act is not None:
                assert self._recorder is not None
                try:
                    if state.replay:
                        raise NodeError("durable replay ended before reproducing committed operations")
                    reading = state.reading
                    if (state.node.on_correction != "rerun" or state.passive or ctx._saved or reading is None
                            or tuple(reading.at) != act.took or state.streams):
                        reading = None
                    self._recorder.act(act, reading=reading, children=state.inner.values())
                    state.bound = False
                    if self._durable is not None:
                        state.rework = node.id in self._durable.rework.waiting
                except Exception as exc:   # noqa: BLE001 - something it sent is not plain data
                    self._fail(state, RunError(f"cannot be journaled: {exc}"))
                    return True
                for journal in state.inner.values():
                    journal.discard()         # the activation is on record: its child journals are no longer needed
            self._release(state)
        return True

    def _discard_stale(self, state: State, act: Act | None) -> bool:
        """End a superseded physical activation while leaving its logical reading ready for rework."""
        assert self._durable is not None and self._recorder is not None and act is not None
        try:
            snapshot = self._durable.data["snaps"].get(state.node.id)
            if snapshot is not None:
                state.node.restore(snapshot)
            self._recorder.act(Act(act.node, act.round, act.took), children=state.inner.values(), stale=True)
            for journal in state.inner.values():
                journal.discard()
        except Exception as exc:
            self._fail(state, exc)
            return True
        state.bound = False
        state.replay.clear()
        self._release(state)
        return True

    def _release(self, state: State) -> None:
        sched = self._sched
        state.running = False
        self._active -= 1
        if state.halt == "restart":    # it finished before it looked: what it did stands, what it holds goes
            state.node.reset()
        state.halt = False
        sched.consider(state)
        if sched.ready or not self._active:
            self._lock.notify_all()

    def _tell(self, event: str, **about) -> None:
        observe = self._observe
        if observe is None:
            return
        try:
            observe({"event": event, **about})
        except Exception as exc:   # noqa: BLE001 - an observer that fails must not go unnoticed
            with self._lock:
                if self._error is None:
                    self._error = RunError(f"the observer failed on {event}: {exc}")
                self._sched.stopped = True
                self._lock.notify_all()

    def _flush(self) -> None:
        """Tell the observer of the problems recorded under the lock."""
        self._checks.flush(self._tell)

    @staticmethod
    def _end_streams(state: State) -> None:
        if state.streams:
            for stream in state.streams.values():
                if stream is not None:
                    stream._close()
            state.streams.clear()

    def _fail(self, state: State, exc: BaseException) -> None:
        """A node failed: the run stops. Called under the lock."""
        self._end_streams(state)
        state.running = False
        self._active -= 1
        if self._error is None:
            self._error = RunError(f"node {state.node.id} failed in round {state.round}: {exc}", state.node.id)
            self._error.__cause__ = exc
        self._sched.stopped = True
        self._lock.notify_all()

    def _close(self) -> list[tuple[State, Any]]:
        """Close every node that was opened. Returns the closings that are still to be awaited."""
        pending = []
        for state in self._states.values():
            if state.opened:
                try:
                    closing = state.node.close(RunContext(self, state, ()))
                    if inspect.isawaitable(closing):
                        pending.append((state, closing))
                except Exception as exc:   # noqa: BLE001
                    self._close_failed(state, exc)
        return pending

    def _close_failed(self, state: State, exc: Exception) -> None:
        if self._error is None:
            self._error = RunError(f"node {state.node.id} failed to close: {exc}", state.node.id)

    # ---- what a node asks of the run through ctx ---------------------------------------------------------

    def _emit(self, state: State, port: str, payload: Any, round: int, cause: tuple, act: Act | None = None) -> str:
        if self._durable is not None:
            if state.redoing and state.redoing.get(port):
                return self._committed_correct(state, state.redoing[port].popleft(), payload, round, cause, act)
            return self._committed_emit(state, port, payload, round, cause, act)
        if state.redoing and state.redoing.get(port):     # run again: it corrects what it sent the first time
            name = state.redoing[port].popleft()
            if name in state.sent_log:
                return self._correct(state, name, payload, round, cause, act)
        try:
            feeds, names = state.outs[port]
        except KeyError:
            raise NodeError(f"{state.node.id} has no output port {port!r}; its outputs are {sorted(state.outs)}") from None
        if port in state.calls:
            raise NodeError(f"{state.node.id}.{port} makes a call: use ctx.call, not emit")
        bodies = self._bodies(state, port, payload, feeds, round, act)
        if self._observe is not None:
            self._tell("emitted", node=state.node.id, round=round, port=port, payload=payload)
        sched = self._sched
        with self._lock:
            if self._over:
                raise NodeError("the run has ended")
            state.delivered = True
            count = state.emitted[port] = state.emitted.get(port, 0) + 1
            token = f"{port}#{count}"
            outputs = []
            for name in names:
                output_index = len(self._result.outputs[name])
                outputs.append([name, output_index])
                self._result.outputs[name].append(payload)
                if act is not None:
                    act.out.append((name, payload))
                    act.out_indices.append(output_index)
            envelopes = []
            # Only an unchanged immutable body can use another edge's digest.
            share_digest = len(feeds) > 1 and type(payload) is str
            shared_digest = None
            for (feed, _), body in zip(feeds, bodies):
                if feed.gone:                           # the graph changed while this was being sent
                    continue
                if feed.stream and feed.id in state.streams:
                    if state.streams[feed.id] is not None:
                        state.streams[feed.id]._put(body)
                    continue
                if self._limited_on(feed, state, act):
                    continue
                if feed.stream:                          # the first chunk of this activation opens the stream
                    stream = state.streams[feed.id] = Stream(feed.protocol, feed.port.port, keep=act is not None)
                    stream._put(body)
                    body = stream
                if share_digest and body is payload:
                    envelope = sched.envelope(feed, body, round, cause, body_digest=shared_digest)
                    shared_digest = envelope.digest
                else:
                    envelope = sched.envelope(feed, body, round, cause)
                if not feed.stream:
                    envelopes.append([feed.id, envelope.id])
                self._post(feed, envelope, act)
            if not any(feed.stream for feed, _ in feeds):
                bounded(state.sent_log, token, Sent(port, envelopes, outputs, round))
                if act is not None:
                    act.emits.append([token, port, envelopes, outputs])
            if state.reading is not None:
                state.reading.emits.setdefault(port, []).append(token)
            self._wake()
        return token

    def _write_durable(self, state: State, record: dict) -> None:
        """Append an operation while holding the engine lock, before exposing its effect."""
        assert self._recorder is not None
        try:
            self._recorder.write(record)
        except (OSError, TypeError, ValueError) as error:
            self._abort_durable(state, error)
            raise NodeError(f"cannot commit durable operation: {error}") from error

    def _abort_durable(self, state: State, error: Exception) -> None:
        if self._error is None:
            self._error = RunError(f"cannot commit durable operation: {error}", state.node.id)
        self._sched.stopped = True
        self._lock.notify_all()

    def _inputs_current(self, state: State) -> bool:
        if self._durable is None:
            return True
        node = state.node.id
        pending = self._durable.pending.get(node)
        return pending is None or self._durable.rework.current(node, pending)

    def _check_current(self, state: State) -> None:
        if not self._inputs_current(state):
            raise Superseded("a bound input was revised")

    def _is_rework(self, state: State) -> bool:
        return self._durable is not None and "redo" in self._durable.pending.get(state.node.id, {})

    @staticmethod
    def _replayed(state: State, operation: dict) -> dict | None:
        if not state.replay:
            return None
        expected = state.replay[0]
        if any(canonical(expected.get(key)) != canonical(value) for key, value in operation.items()):
            raise NodeError(f"durable replay of {state.node.id} diverged at {operation['kind']}")
        return state.replay.popleft()

    def _committed_emit(self, state: State, port: str, payload: Any, round: int, cause: tuple, act: Act | None) -> str:
        if act is None:
            raise NodeError("durable delivery requires an active journaled node")
        try:
            feeds, names = state.outs[port]
        except KeyError:
            raise NodeError(f"{state.node.id} has no output port {port!r}") from None
        if port in state.calls:
            raise NodeError(f"{state.node.id}.{port} makes a call: use ctx.call, not emit")
        key = payload_key(payload)
        with self._lock:
            if self._over or self._sched.stopped:
                raise NodeError("the run is stopping")
            self._check_current(state)
            replayed = self._replayed(state, {"kind": "deliver", "port": port, "key": key})
            if replayed is not None:
                state.delivered = True
                return str(replayed["token"])
        bodies = self._bodies(state, port, payload, feeds, round, act)
        with self._lock:
            if self._over or self._sched.stopped:
                raise NodeError("the run is stopping")
            self._check_current(state)
            count = state.emitted.get(port, 0) + 1
            token = f"{port}#{count}"
            outputs = [[name, len(self._result.outputs[name]), pack_body(copy_body(payload))] for name in names]
            limited: list[str] = []
            deliveries: list[tuple[Feed, Envelope]] = []
            for (feed, _), body in zip(feeds, bodies):
                if feed.gone:
                    continue
                if feed.limit is not None and feed.sent >= feed.limit:
                    limited.append(feed.id)
                else:
                    deliveries.append((feed, self._sched.envelope(feed, copy_body(body), round, cause)))
            self._write_durable(state, {"kind": "deliver", "node": state.node.id, "round": round,
                                        "port": port, "key": key, "token": token, "outputs": outputs,
                                        "limited": limited, "sent": [pack(envelope) for _, envelope in deliveries]})
            state.delivered = True
            state.emitted[port] = count
            self._limited.update(dict.fromkeys(limited))
            for name, _, body in outputs:
                self._result.outputs[name].append(unpack_body(body))
            if self._observe is not None:
                self._tell("emitted", node=state.node.id, round=round, port=port, payload=payload)
            for feed, envelope in deliveries:
                self._sched.place(feed, envelope)
            self._wake()
        return token

    def _bodies(self, state: State, port: str, payload: Any, feeds, round: int, act: Act | None) -> list:
        """What ``payload`` becomes on each of ``feeds``, once it has passed the checks."""
        try:
            bodies = Checks.outgoing(state, port, payload, feeds)
        except ValueError as exc:
            try:
                bodies = self._checks.refused(state, port, payload, feeds, exc, act, round, self._lock)
            finally:
                self._flush()
        carry = self._carry
        if carry is not None:
            bodies = [body if feed.stream else carry(feed.id, body) for (feed, _), body in zip(feeds, bodies)]
        return bodies

    def _limited_on(self, feed: Feed, state: State, act: Act | None) -> bool:
        """Whether ``feed`` has carried all it may (under the lock): then nothing more goes on it."""
        if feed.limit is not None and feed.sent >= feed.limit:
            self._limited[feed.id] = None
            if act is not None and feed.id not in act.limited:
                act.limited.append(feed.id)
            if feed.stream:
                state.streams[feed.id] = None
            return True
        return False

    def _post(self, feed: Feed, envelope, act: Act | None) -> None:
        if act is not None:
            act.sent.append(envelope)
        if envelope.id in self._orphans:         # its receiver dealt with it before the restart
            self._orphans.discard(envelope.id)
        else:
            self._sched.place(feed, envelope)

    def _wake(self) -> None:
        if self._waiters or self._sched.ready:
            # The driver also waits on this condition, so waking just one can leave workers asleep.
            self._lock.notify_all()

    def _correct(self, state: State, name: str, payload: Any, round: int, cause: tuple, act: Act | None) -> str:
        """``ctx.correct``: ``payload`` takes the place of message ``name`` this node sent (see
        ``Context.correct``). Where it waits unread it is replaced; where it was read, the receiver's
        ``on_correction`` says what follows; a graph output it reached is changed."""
        if self._durable is not None:
            return self._committed_correct(state, name, payload, round, cause, act)
        node = state.node
        sent = state.sent_log.get(name)
        if sent is None:
            raise NodeError(f"{node.id} has sent no message {name!r} it can still correct")
        port = sent.port
        feeds = [(self._feeds[feed_id], node.outputs[port]) for feed_id, _ in sent.envelopes if feed_id in self._feeds]
        bodies = self._bodies(state, port, payload, feeds, round, act)
        if self._observe is not None:
            self._tell("corrected", node=node.id, round=round, port=port, message=name, payload=payload)
        with self._lock:
            if self._over:
                raise NodeError("the run has ended")
            state.delivered = True
            for out, index in sent.outputs:
                if out in self._result.outputs and index < len(self._result.outputs[out]):
                    self._result.outputs[out][index] = payload
                    if act is not None:
                        act.fixed_out.append((out, index, payload))
            bodies_by = {feed.id: body for (feed, _), body in zip(feeds, bodies)}
            envelopes = []
            for feed_id, old in sent.envelopes:
                feed = self._feeds.get(feed_id)
                if feed is None or feed.gone:
                    continue
                body = bodies_by[feed_id]
                envelopes.append([feed_id, self._replace(feed, old, body, round, cause, act)])
            sent.envelopes = envelopes
            if act is not None:
                act.emits.append([name, port, envelopes, sent.outputs])
            self._wake()
        return name

    def _committed_correct(self, state: State, token: str, payload: Any, round: int,
                           cause: tuple, act: Act | None) -> str:
        assert self._durable is not None
        if act is None:
            raise NodeError("durable correction requires an active journaled node")
        durable, node = self._durable, state.node
        key = payload_key(payload)
        operation = {"kind": "correct", "token": token, "key": key}
        with self._lock:
            self._check_current(state)
            if self._replayed(state, operation) is not None:
                state.delivered = True
                return token
            message = durable.rework.messages.get(node.id, {}).get(token)
            if message is None:
                raise NodeError(f"{node.id} has sent no message {token!r} it can still correct")
            port = message["port"]
            feeds = [(self._feeds[id], node.outputs[port]) for id in message["envelopes"]]
        changed = message["key"] != key
        bodies = self._bodies(state, port, payload, feeds, round, act) if changed else []
        with self._lock:
            if self._over or self._sched.stopped:
                raise NodeError("the run is stopping")
            self._check_current(state)
            outputs = [[name, index, pack_body(copy_body(payload))] for name, index in message["outputs"]] if changed else []
            deliveries: list[tuple[Feed, Envelope, str]] = []
            limited: list[str] = []
            for (feed, _), body in zip(feeds, bodies):
                old = message["envelopes"][feed.id]
                route = self._correction_route(feed, old)
                if route == "ignore":
                    continue
                if feed.limit is not None and feed.sent >= feed.limit:
                    limited.append(feed.id)
                    continue
                envelope = self._sched.envelope(feed, copy_body(body), round, cause, old)
                deliveries.append((feed, envelope, route))
            self._write_durable(state, {**operation, "node": node.id, "round": round, "port": port,
                                       "version": message["version"] + int(changed), "noop": not changed,
                                       "outputs": outputs, "limited": limited,
                                       "sent": [pack(envelope) for _, envelope, _ in deliveries],
                                       "routes": [route for _, _, route in deliveries]})
            state.delivered = True
            self._limited.update(dict.fromkeys(limited))
            for name, index, body in outputs:
                self._result.outputs[name][index] = unpack_body(body)
            for feed, envelope, route in deliveries:
                self._apply_correction(feed, envelope, route)
            self._wake()
        if changed and self._observe is not None:
            self._tell("corrected", node=node.id, round=round, port=port, message=token, payload=payload)
        return token

    def _correction_route(self, feed: Feed, envelope: str) -> str:
        assert self._durable is not None
        rework = self._durable.rework
        reader = rework.readers.get(envelope)
        if reader is None:
            return "unread" if envelope in self._durable.data["mail"] else "ignore"
        node, round = reader
        policy = feed.port.state.node.on_correction
        if policy == "ignore":
            return "ignore"
        if policy == "rerun" and rework.readings[node][round]["replayable"]:
            return "rerun"
        return "tell"                      # a saved continuation or graph edit cannot be repeated

    def _apply_correction(self, feed: Feed, envelope: Envelope, route: str) -> None:
        if route == "unread":
            at = next(i for i, waiting in enumerate(feed.box) if waiting.id == envelope.replaces)
            feed.box[at] = envelope
        elif route == "rerun":
            receiver = feed.port.state
            receiver.rework = True
            self._sched.consider(receiver)
        else:
            self._sched.place(feed, envelope)

    @staticmethod
    def _reread(receiver: State, feed: Feed, old: str, envelope: Envelope, value: Any) -> None:
        """What ``receiver`` read as ``old`` is now ``value``, which came in ``envelope``. Under the lock."""
        new = envelope.id
        reading = receiver.read_log.pop(old, None)
        bounded(receiver.read_log, new, reading)
        if reading is not None:
            reading.update(feed, old, envelope, value)

    @staticmethod
    def _retook(took: list | None, feed: Feed, old: str, envelope: Envelope) -> None:
        """What was taken as ``old`` came again, corrected, as ``envelope``."""
        for i, (_, was) in enumerate(took or ()):
            if was.id == old:
                took[i] = (feed, envelope)    # type: ignore[index]

    def _replace(self, feed: Feed, old: str, body: Any, round: int, cause: tuple, act: Act | None) -> str:
        """Correct envelope ``old`` on ``feed`` with ``body``, under the lock. Returns the id of the envelope
        that stands for the message now (``old`` itself when nothing could be sent)."""
        box, sched = feed.box, self._sched
        at = next((i for i, waiting in enumerate(box) if waiting.id == old), None)
        receiver = feed.port.state
        if at is None and old not in receiver.read_log:
            return old                          # dropped (a newer value took its place), or read too long ago
        if self._limited_on(feed, receiver, act):
            return old
        if at is not None:                       # not read yet: the correction takes its place
            envelope = sched.envelope(feed, body, round, cause)
            box[at] = envelope
            if act is not None:
                act.sent.append(envelope)
                act.fixed_box.append(old)
            return envelope.id
        awaiting = receiver.reviews.awaiting
        if awaiting is not None and awaiting.envelope == old and awaiting.answer is None:
            # the receiver rejected it and waits: the correction is its answer, and nothing else follows
            envelope = sched.envelope(feed, body, round, cause, old)
            if act is not None:
                act.sent.append(envelope)
                act.fixed_box.append(envelope.id)      # it is taken as the answer, not delivered again
            try:
                value = self._checks.arrived(receiver, feed, body)
            except Exception as exc:   # noqa: BLE001
                awaiting.settle("refused", f"the correction could not be read: {exc}")
                return envelope.id
            self._reread(receiver, feed, old, envelope, value)
            self._retook(receiver.took_on.get(feed.port.port.name), feed, old, envelope)
            awaiting.settle("corrected", value=value)
            if awaiting.feed.source is not None:
                awaiting.feed.source.reviews.open.pop(awaiting.id, None)
            self._lock.notify_all()
            return envelope.id
        rule = receiver.node.on_correction
        if rule == "ignore":
            return old
        reading = receiver.read_log[old]
        if rule == "rerun" and reading is not None:
            envelope = sched.envelope(feed, body, round, cause, old)
            if act is not None:
                act.sent.append(envelope)
                act.redos.append(envelope.id)
            try:
                value = self._checks.arrived(receiver, feed, body)
            except Exception as exc:   # noqa: BLE001 - a protocol's decode failed: the receiver fails, as it would
                self._fail(receiver, exc)
                return envelope.id
            self._reread(receiver, feed, old, envelope, value)
            if not reading.queued:
                reading.queued = True
                receiver.redo.append(reading)
                sched.consider(receiver)
            return envelope.id
        envelope = sched.envelope(feed, body, round, cause, old)     # told: it comes as a message of its own
        self._post(feed, envelope, act)
        return envelope.id

    def _call(self, state: State, port: str, payload: Any, act: Act | None = None) -> Any:
        """Run the node at the other end of a call edge now, in this thread, and return its answer."""
        feed, target = self._callee(state, port)
        called = target.called
        assert called is not None
        with called:
            return through(self._calling(state, port, feed, payload, act), waited)

    async def _acall(self, state: State, port: str, payload: Any, act: Act | None = None) -> Any:
        """``_call``, awaited: a called node that is a coroutine is awaited, and the loop is not held up while
        the called node is busy with another caller."""
        feed, target = self._callee(state, port)
        called = target.called
        assert called is not None
        while not called.acquire(blocking=False):
            await asyncio.sleep(0.001)
        try:
            return await athrough(self._calling(state, port, feed, payload, act))
        finally:
            called.release()

    def _callee(self, state: State, port: str) -> tuple[Feed, State]:
        feed: Feed | None = state.calls.get(port)
        if feed is None:
            raise NodeError(f"{state.node.id} has no call port {port!r}")
        assert feed.port.state.called is not None
        return feed, feed.port.state

    def _calling(self, state: State, port: str, feed: Feed, payload: Any, act: Act | None):
        """One call, as a generator that yields what is to be awaited and is sent what it came to; the
        caller holds the called node's lock."""
        body = feed.protocol.encode(payload, state.node.outputs[port])
        target = feed.port.state
        node = target.node
        with self._lock:
            if self._sched.stopped:
                raise NodeError("the run is stopping")
            if target.status != ACTIVE:
                raise NodeError(f"{node.id} is {target.status}: it cannot answer now")
            feed.sent += 1
            target.round += 1
            self._sched.envelopes += 1
            self._result.activations += 1
            ctx = RunContext(self, target, (f"{feed.id}#{feed.sent}",), act)
            if act is not None:
                act.calls.append(node.id)
            target.rejected = {}
        if self._observe is not None:
            self._tell("started", node=node.id, round=ctx.round, took=list(ctx._cause), by=state.node.id)
        failed = True
        try:
            if not target.opened:
                target.opened = True
                opened = node.open(ctx)
                if inspect.isawaitable(opened):
                    yield opened
            value = feed.protocol.decode(body, feed.port.port)
            if feed.guard is not None:
                feed.guard.check(value)
            returned = node.run(ctx, {feed.port.port.name: [value] if feed.port.port.many else value})
            if inspect.isawaitable(returned):
                returned = yield returned
            if ctx._cut:            # it was told to stop: the caller must not take what it left for an answer
                raise NodeError("the run is stopping")
            self._snap(node, act)
            if returned is None:
                failed = False
                return None
            if not isinstance(returned, Mapping):
                raise NodeError(f"run must return a mapping of output port to payload, or None; got {type(returned).__name__}")
            for name, item in returned.items():
                self._emit(target, name, item, ctx.round, ctx._cause, act)
            failed = False
            return returned.get(feed.reply) if feed.reply is not None else None
        except Invalid as exc:      # the called node did not put its own output right: that is its failure,
            raise NodeError(f"{node.id} could not answer: {exc}") from None      # not something for the caller to repair
        finally:
            if self._observe is not None:
                self._tell("failed" if failed else "finished", node=node.id, round=ctx.round, by=state.node.id)
            with self._lock:
                self._end_streams(target)

    def _inner(self, ctx: RunContext, graph: Graph, protocols: Protocols | None, workers: int, firm: bool = False) -> "Engine":
        """An engine for a graph that runs inside one activation of one of this engine's nodes."""
        state = ctx._state
        journal = None
        if self._recorder is not None:
            journal = self._recorder.inner(state.node.id, ctx.round)
            with self._lock:
                state.inner[ctx.round] = journal
        told, id = self._observe, state.node.id
        observe = None if told is None else lambda event: told({**event, "within": [id, *event.get("within", ())]})
        inner = Engine(graph, protocols, workers=workers, stopping=ctx.stopping, journal=journal,
                       time_limit=self._limit, observe=observe, firm=firm or self._firm, rebuild=self._rebuild,
                       max_nodes=self._max_nodes)
        inner._parent = (self, state, ctx._act)
        inner._within = (*self._within, (state.node.id, ctx.round))
        inner._id = ctx.key
        with self._lock:
            self._inside[state.node.id] = inner     # its nodes can be reached as "<node>/<id>"
        return inner

    def _use(self, state: State, round: int, used: Mapping[str, float]) -> None:
        """Add what a node reports using to the run's count; over the budget, the run fails."""
        used = amounts(used, "use")
        if not used:
            return
        with self._lock:
            if self._over:
                return
            over = self._ledger.add(used)
            if self._recorder is not None:
                self._recorder.use(state.node.id, round, used)
            if over and self._error is None:        # the run ends even if the node goes on regardless
                self._error = RunError(over, state.node.id)
                self._sched.stopped = True
                self._lock.notify_all()
            total = dict(self._ledger.meter)
        if self._observe is not None:
            self._tell("used", node=state.node.id, round=round, amounts=used, total=total)
        if self._parent is not None:
            outer, outer_state, _ = self._parent
            outer._use(outer_state, self._within[-1][1], used)
        if over:
            raise OverBudget(over)

    def _once(self, state: State, round: int, name: str, do: Callable[[], Any]) -> Any:
        """What ``do`` returned the first time this activation asked under this name; ``do`` is called only then."""
        if not isinstance(name, str) or not name:
            raise NodeError("once needs a name for what is done")
        ledger, id = self._ledger, state.node.id
        with self._lock:
            if ledger.has(id, round, name):
                return ledger.get(id, round, name)
        value = do()
        with self._lock:
            if self._recorder is not None:
                try:
                    self._recorder.once(id, round, name, value)
                except (TypeError, ValueError) as exc:
                    raise NodeError(f"what once keeps under {name!r} must be plain data: {exc}") from None
            ledger.keep(id, round, name, value)
        return value

    def _change(self, state: State, change: Change, act: Act | None) -> None:
        """Change the graph now, as one of its nodes asks: refused whole, or made whole."""
        if not isinstance(change, Change):
            raise NodeError(f"a change to the graph is a Change, not {type(change).__name__}")
        if not change:
            return
        described = None
        if self._recorder is not None:
            if self._rebuild is None and not all(self._remakeable(node) for node in change.added):
                raise GraphError("this run is journaled, so a node it adds must be made again when the run is taken "
                                 "up: give the engine rebuild=")
            described = change.describe()
            try:
                canonical(described)
            except (TypeError, ValueError) as exc:
                raise GraphError(f"the change cannot be journaled: {exc}") from None
        with self._lock:
            if self._over or self._sched.stopped:
                raise NodeError("the run is stopping")
            self._ending(state, change)
            before = {node.id for node in self.graph.nodes}
            if self._durable is not None:
                if act is None or described is None or any(
                        op[0] not in ("add", "connect", "ports", "output", "manage") for op in described):
                    raise GraphError("durable graph edits must add nodes, edges, ports, outputs or management")
                self._check_current(state)
                if self._is_rework(state):
                    raise GraphError("a rework activation cannot edit the graph")
                operation = {"kind": "topology", "operations": described}
                if self._replayed(state, operation) is not None:
                    return
                dropped = self._make(change, state.node.id, before_apply=lambda: self._write_durable(
                    state, {**operation, "node": state.node.id, "round": state.round}))
            else:
                dropped = self._make(change, state.node.id)
            if act is not None and self._durable is None:
                act.changes.append(described)
            if self._sched.ready:
                self._lock.notify_all()
            now = {node.id for node in self.graph.nodes}
        if self._observe is not None:
            self._tell("changed", node=state.node.id, round=state.round, added=sorted(now - before),
                       removed=sorted(before - now), dropped=dropped, graph=self.graph.digest)

    def _ending(self, state: State, change: Change, patience: float = 5.0) -> None:
        """Nodes to be taken out that were stopped and have not yet ended: wait a little for them, holding no
        worker, so that taking out a member just stopped does not fail. Under the lock."""
        def stopping() -> bool:
            return any(other is not None and other.running and other.status == STOPPED
                       for other in map(self._states.get, change.removed))
        if state.node.asynchronous or not stopping():
            return                              # a coroutine would hold the loop they may need
        until = time.monotonic() + patience
        self._lend(state)
        self._lock.notify_all()
        try:
            while stopping() and not self._over and time.monotonic() < until:
                self._lock.wait(0.02)
        finally:
            self._take_back(state)

    def _remakeable(self, node: Node) -> bool:
        """Whether a node added at run time can be made again without ``rebuild``: its maker (an agent that hired
        it) is in the graph and can make it again from the same template."""
        made_by = getattr(node, "made_by", None)
        if not made_by:
            return False
        maker = next((n for n in self.graph.nodes if n.id == made_by.get("node")), None)
        return maker is not None and callable(getattr(maker, "remake", None))

    def _remade(self, described: dict) -> Node:
        """A node added at run time, made again by what made it (``rebuild``, when given, comes first)."""
        if self._rebuild is not None:
            return self._rebuild(described)
        made_by = described.get("made_by") or {}
        maker = next((n for n in self.graph.nodes if n.id == made_by.get("node")), None)
        if maker is None or not callable(getattr(maker, "remake", None)):
            raise GraphError(f"node {described.get('id')!r} was added while the run went on, and nothing was given "
                             f"that can make it again (rebuild)")
        return getattr(maker, "remake")(made_by["template"], described["id"])

    def _remake(self, described: list) -> int:
        """Make again, while the run is being taken up, a change the journal holds."""
        return self._make(Change.of(described, self._remade), None, consider=False)

    def _make(self, change: Change, caller: str | None, consider: bool = True,
              before_apply: Callable[[], None] | None = None) -> int:
        """Apply ``change`` to the graph and to what the run keeps of it. Called under the lock. Returns how
        many waiting envelopes were dropped with the edges taken out. While a run is being taken up nothing
        is queued yet (``consider=False``): that is done once the whole journal has been read."""
        old, wiring, sched = self.graph, self._wiring, self._sched
        if caller is not None and caller in change.removed:
            raise GraphError(f"{caller} cannot take itself out of the graph")
        try:
            new = change.apply(GraphBuilder.of(old)).compile(self.protocols)
            if self._max_nodes is not None and len(new.nodes) > self._max_nodes:
                raise GraphError(f"the graph would have {len(new.nodes)} nodes, and at most {self._max_nodes} may "
                                 f"run here")
            known = {node.id for node in new.nodes}
            gone = [id for id in self._states if id not in known]
            for id in gone:
                if self._states[id].running:
                    raise GraphError(f"{id} is running and cannot be taken out now")
            if self._durable is not None:
                self._check_durable_graph(new)
                if any(new.outputs.get(name) != endpoint for name, endpoint in old.outputs.items()):
                    raise GraphError("durable graph edits may only add output names")
            if before_apply is not None:
                before_apply()
        except Exception:
            change.undo()                       # ports it gave to a node stay with the graph that was refused
            raise
        edges = {edge.id: edge for edge in new.edges}
        dropped = sum(wiring.remove_edge(edge.id) for edge in old.edges if edge.id not in edges)
        for name in [name for name in old.inputs if name not in new.inputs]:
            dropped += wiring.remove_edge(INPUT + name)
            del wiring.inputs[name]
        for id in gone:
            state = self._states[id]
            self._unheard(state, "the sender was taken out of the graph, and cannot answer")
            if state.queued:
                sched.ready.remove(state)
            wiring.remove_node(id)
        added = [wiring.add_node(node) for node in new.nodes if node.id not in wiring.states]
        for node, names in change.grown:            # new input ports of a node already in the run
            wiring.add_ports(wiring.states[node.id], names)
        set_limits(added, self._limit, self._firm)
        before = {edge.id for edge in old.edges}
        for edge in new.edges:
            if edge.id not in before:
                wiring.add_edge(edge)
        wiring.settle(new)
        for name in new.outputs:
            self._result.outputs.setdefault(name, [])
        self.graph = new
        if consider:
            for state in self._states.values():
                sched.consider(state)
        return dropped

    def _adopt(self, state: State, act: Act | None, problems: list[dict]) -> None:
        """Take on the problems of a run that happened inside one of this engine's nodes."""
        with self._lock:
            for problem in problems:
                record = {**problem, "node": f"{state.node.id}/{problem['node']}"}
                self._result.problems.append(record)
                if act is not None:
                    act.problems.append(record)

