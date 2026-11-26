"""The node: anything that takes information in and gives information out.

A node knows its ports and how to run once. It does not know which graph it is in, who is upstream or
downstream, or how its outputs travel: everything it touches outside itself goes through the ``Context`` the
engine hands it. An agent, a function, an external program, a device, a person and a whole sub-graph are all
nodes, with no difference in standing.
"""
from __future__ import annotations

import asyncio
import inspect
import re
from abc import ABC, abstractmethod
from enum import Enum
from types import MappingProxyType
from typing import Any, ClassVar, Iterable, Mapping

from .port import Port

ID = re.compile(r"[a-z][a-z0-9_-]{0,63}")


class NodeError(ValueError):
    pass


class Invalid(NodeError):
    """What a node sent, or was sent, did not pass a check. ``problem`` says what is wrong, in words meant
    to be read by whoever can put it right. ``again`` is true when the node may correct it and send again
    within the same activation; when it is false the node fails."""

    def __init__(self, port: str, problem: str, again: bool = False):
        super().__init__(f"{port}: {problem}")
        self.port, self.problem, self.again = port, problem, again


class Firing(Enum):
    """When a node is activated."""

    ALL = "all"   # once every required input has arrived
    ANY = "any"   # each time any input arrives


class Context(ABC):
    """What the engine gives a node for one activation."""

    node: str     # the node's id
    round: int    # which activation of this node this is, from 1
    problems: list  # when the activation is run again because what it sent was not accepted: what was wrong
    within: Any     # when the graph runs inside a node of another: (node id, round) of each, outermost first
    key: Any        # names this activation of this node in this run, and is the same when it runs again after
                    # an interruption: give it to an outside service so that it does not do the same thing twice

    @abstractmethod
    def emit(self, port: str, payload: Any) -> Any:
        """Send ``payload`` out of an output port now (a node may emit many times during one activation).
        Raises ``Invalid`` when the payload does not pass the port's checks; a node that can put it right
        catches that, reads ``problem`` and, when ``again`` is true, sends a corrected payload. Returns what
        names this message (``"out#3"``) for ``correct``, where the context can correct."""

    def correct(self, sent: str, payload: Any) -> str:
        """Correct a message this node sent earlier (``sent`` is what ``emit`` returned): ``payload`` takes
        its place. Where it has not been read yet, it is replaced; where it has, each receiver deals with it
        as its ``on_correction`` says. A graph output it reached is corrected too. It is checked like any
        message, journaled, and may be corrected again: the last one counts."""
        raise NotImplementedError("this context cannot correct what was sent")

    def replaces(self, port: str) -> str | None:
        """When the last message this activation took on ``port`` corrects one read earlier: that one's id."""
        return None

    def challengeable(self, port: str, which: str | None = None) -> bool:
        """Whether what this activation took on ``port`` (the last, or the one ``which`` names) may still be
        rejected (see ``reject``)."""
        return False

    def taken(self, port: str) -> list:
        """What this activation took on ``port``, in order: each says ``id`` (of the envelope), ``from`` (the
        node that sent it) and ``edge``. Any of them names the message to ``reject`` on a port fed by several."""
        return []

    def reject(self, port: str, reason: str, which: str | None = None) -> dict:
        """Say that what this activation took on ``port`` is not what was expected, where its edge takes
        rejections (``challenges``), and wait until the sender answers. It is the last message taken there,
        or the one ``which`` names: its envelope id, its edge, or the node that sent it (see ``taken``).
        Returns ``{"answer": "explained", "text": ...}`` (the value stands), ``{"answer": "corrected",
        "value": ...}`` (the sender corrected it: this is the new value) or ``{"answer": "refused", "why":
        ...}`` (the edge has taken all the rejections it may, the sender cannot answer, or nothing more can
        happen)."""
        raise NotImplementedError("this context cannot reject what came")

    def rejections(self) -> list:
        """Rejections of what this node sent, not yet answered: each says ``id``, ``port``, ``message`` (the
        name ``emit`` gave), ``content``, ``reason``, ``by`` and ``sent`` (output port -> the names of what
        else the same activation sent, which may be wrong too). Answer each with ``explain`` or ``correct``,
        and correct those others as well where they need it; one left unanswered when the activation ends is
        answered by ``Node.challenged``. ``receive`` returns an empty mapping when one comes while it waits."""
        return []

    def failures(self) -> list:
        """Failures of nodes it manages that came to it (its node ``oversees``), not yet taken up: each says
        ``node``, ``round`` and ``error``. The node that failed is stopped; switch it (``restart``), take it
        out, or fail the activation to fail the run. Taken up, they are this node's to deal with; one left
        when the activation ends fails the run as it would have. ``receive`` returns an empty mapping when
        one comes while it waits."""
        return []

    def explain(self, rejection: str, text: str) -> None:
        """Answer rejection ``rejection`` (its id): what was sent stands, for this reason."""
        raise NotImplementedError("this context cannot answer rejections")

    @abstractmethod
    def call(self, port: str, payload: Any) -> Any:
        """Send ``payload`` out of an output port whose edge is a call, wait, and return the other node's answer."""

    @abstractmethod
    def stopping(self) -> bool:
        """True once the run is being stopped: a long activation should return soon."""

    @abstractmethod
    def engine(self, graph: Any, protocols: Any = None, *, workers: int = 0, firm: bool = False) -> Any:
        """An engine for a graph that runs inside this activation. It stops when the outer run stops, keeps
        its own journal beside the outer one when there is one, tells the outer observer what happens in it,
        and works under the same time limit. ``go(inputs)`` runs it, or takes it up where an interrupted
        earlier attempt at this same activation left it. ``firm=True`` is for a graph that is not trusted:
        none of its nodes may give itself more time than the run allows."""

    async def acall(self, port: str, payload: Any) -> Any:
        """``call``, for a node whose ``run`` is a coroutine: the event loop stays free while the other node
        answers."""
        return self.call(port, payload)

    def time_left(self) -> float | None:
        """Seconds until this activation's time limit; None when it has none."""
        return None

    #: True when this activation goes on from progress an earlier one saved with ``commit``: the node's own
    #: state is as it was then, the inputs are empty, and what has come since is there to ``receive``.
    resumed = False

    def commit(self) -> bool:
        """Save the progress of a long activation: what it has taken, sent and holds (its ``snapshot``) is
        put on record now, and the activation goes on. Should the process end, ``resume`` does not run it
        again from the start: the node is given back its state as saved and runs once more with ``resumed``
        true and no inputs, to go on from there. What it took before the save is no longer given back when
        it is stopped. Returns whether it was journaled (False when the run keeps no journal)."""
        return False

    def receive(self, ports: Any = None, timeout: float | None = None) -> dict | None:
        """Wait, in the middle of an activation, for the next message to this node, and take it:
        ``{port: value}``, from the messages that came while it ran, in the order they came (``ports`` keeps
        to some input ports). A node that waits here holds none of the run's workers.

        None when there is no message to wait for: ``timeout`` seconds passed; the node was stopped or
        restarted, or the run is ending (``stopping()`` says so); or nothing more can come, because every other
        activation has ended or is waiting too. Two nodes waiting for each other both get None, and can
        finish. What is taken here belongs to the activation, as what it started with does."""
        raise NotImplementedError("this context cannot receive in the middle of an activation")

    async def areceive(self, ports: Any = None, timeout: float | None = None) -> dict | None:
        """``receive``, for a node whose ``run`` is a coroutine."""
        raise NotImplementedError("this context cannot receive in the middle of an activation")

    def paused(self) -> bool:
        """True while this node is paused and has not yet been told to end. For a node that hands its work to
        something outside (a program, a server) and wants to pause that too; most nodes need only
        ``checkpoint``."""
        return False

    def checkpoint(self) -> bool:
        """Say that a step is done (a call to a model, a tool, one item of a long list). While this node is
        paused it waits here; it returns False when the activation should end now, because the run is stopping
        or this node was stopped or restarted. A node that never calls it is paused between activations."""
        return not self.stopping()

    async def acheckpoint(self) -> bool:
        """``checkpoint``, for a node whose ``run`` is a coroutine."""
        return not self.stopping()

    def control(self, nodes: Any, action: str, *, team: bool = False) -> list:
        """Pause, resume, stop or restart nodes this node manages (``GraphBuilder.manage``); ``team`` takes in
        those they manage. Returns the nodes whose switch changed."""
        raise NotImplementedError("this context cannot control other nodes")

    def team(self) -> dict:
        """The nodes this node manages, all the way down: for each, ``status`` (active, paused or stopped),
        whether it is ``running`` and ``waiting``, its ``manager`` and how many ``rounds`` it has run."""
        return {}

    def once(self, name: str, do: Any) -> Any:
        """Do something outside the graph once for this activation, however often the activation runs.

        ``do()`` is called and what it returns is kept under ``name``, in the journal when the run has one.
        When this activation runs again (the run was interrupted and taken up, or what the node sent was not
        accepted) ``once`` returns what was kept and does not call ``do`` again. What ``do`` returns must be
        plain data. If the process dies after ``do`` has acted and before its result is kept, it is done
        again: for that last gap, give the outside service ``ctx.key`` as well."""
        return do()

    def change(self, change: Any) -> None:
        """Change the graph this node runs in (a ``Change``): checked like a graph being built, and either
        refused with ``GraphError`` (nothing changes) or made at once. With a journal it is kept with this
        activation and made again when the run is taken up."""
        raise NotImplementedError("this context cannot change its graph")

    def progress(self, message: str) -> None:
        """Publish a short display message. An interrupted activation can report the same stage again."""

    def use(self, **amounts: float) -> None:
        """Report what this activation used up, under names of the node's choosing (``tokens=1200``,
        ``calls=1``, ``seconds=3.5``). The run adds it up; when a total goes over the run's budget for that
        name the run fails, and this raises so that the node stops at once."""


def _ports(given: Iterable[Port] | Mapping[str, Port] | None, declared: Iterable[Port] | Mapping[str, Port]) -> Mapping[str, Port]:
    ports = list(given.values() if isinstance(given, Mapping) else given) if given is not None else list(declared)
    out: dict[str, Port] = {}
    for port in ports:
        if not isinstance(port, Port):
            raise NodeError(f"not a port: {port!r}")
        if port.name in out:
            raise NodeError(f"duplicate port: {port.name}")
        out[port.name] = port
    return MappingProxyType(out)


class Node(ABC):
    """Subclass, set ``kind``, declare ports (on the class or per instance) and implement ``run``.

    ``run`` receives the inputs that have arrived, by port name (a list for a ``many`` port). It may return a
    mapping of output port to payload, call ``ctx.emit`` any number of times, or both. Returning ends the
    activation.
    """

    kind: ClassVar[str] = ""                 # the name this kind of node is registered and stored under
    # A class may declare its ports as a tuple of Ports; on an instance they are a mapping from name to Port.
    inputs: Mapping[str, Port] = ()     # type: ignore[assignment]
    outputs: Mapping[str, Port] = ()    # type: ignore[assignment]
    firing: Firing = Firing.ALL
    repairs: int = 0                         # how often an output may be corrected, for ports that do not say
    time_limit: float | None = None          # the longest one activation may take, in seconds; None: the engine's
    #: What the node does when a message it has already read is corrected by its sender: ``rerun`` the
    #: activation that read it with the corrected value, and send what comes out as corrections of what it
    #: sent then; ``tell``, give it the correction as a new message (``ctx.replaces`` says it is one); or
    #: ``ignore`` it. A message not yet read is simply replaced, whatever this says.
    on_correction: str = "rerun"
    #: When a node it manages fails while this node is running, the failure comes to it (``ctx.failures``)
    #: instead of failing the run: the member is stopped and this node decides what follows.
    oversees: bool = False
    made_by: dict | None = None              # for a node an agent hired: {"node": who hired it, "template": from which}

    def __init__(self, id: str, *, inputs: Iterable[Port] | None = None, outputs: Iterable[Port] | None = None,
                 firing: Firing | None = None):
        if not isinstance(id, str) or not ID.fullmatch(id):
            raise NodeError(f"invalid node id: {id!r}")
        if not type(self).kind:
            raise NodeError(f"{type(self).__name__} has no kind")
        self.id = id
        self.inputs = _ports(inputs, type(self).inputs)
        self.outputs = _ports(outputs, type(self).outputs)
        if firing is not None:
            self.firing = Firing(firing)

    def config(self) -> Mapping:
        """Plain data that sets this node apart from another of the same kind (it goes into the graph digest)."""
        return {}

    def describe(self) -> dict:
        own = self.__dict__.get("time_limit")
        rule = self.__dict__.get("on_correction")
        oversees = self.__dict__.get("oversees")
        return {"id": self.id, "kind": type(self).kind, "firing": self.firing.value,
                "inputs": [p.describe() for p in self.inputs.values()],
                "outputs": [p.describe() for p in self.outputs.values()], "config": dict(self.config()),
                **({"time_limit": own} if own is not None else {}),
                **({"on_correction": rule} if rule is not None and rule != type(self).on_correction else {}),
                **({"oversees": oversees} if oversees is not None and oversees != type(self).oversees else {})}

    # ---- lifecycle: only run is required --------------------------------------------------------------

    def open(self, ctx: Context) -> Any:
        """Acquire what the node needs, once, before its first activation. May be ``async def``."""

    @abstractmethod
    def run(self, ctx: Context, inputs: Mapping[str, Any]) -> Mapping[str, Any] | None:
        """One activation. It may be ``async def``: the engine then awaits it on its event loop, so that many
        such activations wait at once without a thread each, and one that overruns its time is cancelled."""

    @property
    def asynchronous(self) -> bool:
        """Whether an activation is a coroutine to be awaited. True when ``run`` is ``async def``."""
        return inspect.iscoroutinefunction(self.run)

    def close(self, ctx: Context) -> Any:
        """Release what ``open`` acquired. May be ``async def``."""

    def add_ports(self, inputs: Iterable[Port] = (), outputs: Iterable[Port] = ()) -> None:
        """Give the node more ports while a run goes on (``Change.ports``). A node that keeps something per
        port (an agent's tools) extends this to learn of them."""
        self.inputs = _ports([*self.inputs.values(), *inputs], ())
        self.outputs = _ports([*self.outputs.values(), *outputs], ())

    def drop_ports(self, names: Iterable[str]) -> None:
        """Take back ports ``add_ports`` gave, when the change that gave them was refused."""
        names = set(names)
        self.inputs = _ports([p for p in self.inputs.values() if p.name not in names], ())
        self.outputs = _ports([p for p in self.outputs.values() if p.name not in names], ())

    def challenged(self, ctx: Context, rejection: Mapping) -> Any:
        """Something this node sent was rejected (``rejection`` says what, as ``Context.rejections`` does) and
        no activation of its own took it up: answer it, by ``ctx.correct`` or by returning an explanation.
        A node that does neither lets what it sent stand; this one says so."""
        return "it was worked out by a program from what it was given, and stands as it is"

    def reset(self) -> None:
        """Forget what the node holds from earlier activations: it is being restarted."""

    def snapshot(self) -> Mapping:
        """Plain data from which ``restore`` can rebuild the node's own state after a restart."""
        return {}

    def restore(self, state: Mapping) -> None:
        pass

    def __repr__(self):
        return f"<{type(self).__name__} {self.id}>"


def waited(value: Any) -> Any:
    """``value``, or, when it is something to await (what an ``async def`` ``run`` or ``open`` gives), what it
    comes to: run to its end on an event loop of its own. Where a loop is already running in this thread that
    cannot be done, and the node must be awaited instead (``await ctx.acall``, ``await engine.arun``)."""
    if not inspect.isawaitable(value):
        return value
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(_awaiting(value))
    close = getattr(value, "close", None)
    if close is not None:
        close()                             # it will never run: say so quietly
    raise NodeError("a coroutine cannot be waited for here, inside a running event loop: await it "
                    "(ctx.acall, engine.arun)")


async def _awaiting(value: Any) -> Any:
    return await value
