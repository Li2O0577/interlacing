"""The graph: nodes joined by edges, checked once and then read-only.

``GraphBuilder`` collects nodes and edges; ``compile`` checks the whole and returns a ``Graph`` with its
indexes built. A graph never changes: a different graph is a new value with a new digest. Building costs
time proportional to nodes plus edges (plus one walk per back edge); every lookup afterwards is a dictionary
or tuple access.

Rules checked by ``compile``:

* every edge joins an existing output port to an existing input port, under a registered protocol that
  accepts the pair;
* an input that is not ``many`` has at most one forward feed (an edge or a graph input); back edges may
  feed it as well, since they only arrive in later rounds, and so may any number of calls;
* a required input has a forward feed, otherwise its node could never start;
* without its back edges the graph has no cycle, and every back edge really closes a loop. With the
  ``max_rounds`` every back edge carries, no run can go on for ever;
* who manages whom (``manage``) is a tree of existing nodes: a node has at most one manager, and no node
  manages itself, directly or through others.
"""
from __future__ import annotations

from collections import deque
from types import MappingProxyType
from typing import Iterable, Mapping

from .canonical import digest
from .edge import Edge, EdgeError, Endpoint
from .node import Firing, Node
from .protocol import Delivery, ProtocolError, Protocols, resolve


class GraphError(ValueError):
    pass


class Graph:
    """A compiled graph. Construct it with ``GraphBuilder.compile``."""

    __slots__ = ("nodes", "edges", "inputs", "outputs", "order", "digest", "protocols", "managers", "_node", "_edge", "_out",
                 "_in", "_needs", "_teams")

    def __init__(self, nodes, edges, inputs, outputs, order, out_index, in_index, needs, managers=None):
        self.nodes: tuple[Node, ...] = nodes
        self.edges: tuple[Edge, ...] = edges
        self.inputs: Mapping[str, Endpoint] = MappingProxyType(inputs)     # the graph's own inputs, by name
        self.outputs: Mapping[str, Endpoint] = MappingProxyType(outputs)   # the graph's own outputs, by name
        self.order: tuple[str, ...] = order                                # node ids, upstream first (back edges ignored)
        self._node = {node.id: node for node in nodes}
        self._edge = {edge.id: edge for edge in edges}
        self._out, self._in, self._needs = out_index, in_index, needs
        self.managers: Mapping[str, str] = MappingProxyType(dict(managers or {}))   # node id -> the node that manages it
        teams: dict[str, list[str]] = {}
        for subordinate, manager in self.managers.items():
            teams.setdefault(manager, []).append(subordinate)
        self._teams = {manager: tuple(sorted(members)) for manager, members in teams.items()}
        self.digest: str = digest(self.describe())
        self.protocols: Protocols | None = None     # what it was compiled with: an engine runs it with these by default

    # ---- lookups ---------------------------------------------------------------------------------------

    def node(self, id: str) -> Node:
        try:
            return self._node[id]
        except KeyError:
            raise GraphError(f"no node {id!r}") from None

    def edge(self, id: str) -> Edge:
        try:
            return self._edge[id]
        except KeyError:
            raise GraphError(f"no edge {id!r}") from None

    def __contains__(self, id) -> bool:
        return id in self._node

    def __len__(self) -> int:
        return len(self.nodes)

    def outgoing(self, node: str, port: str | None = None) -> tuple[Edge, ...]:
        """Edges leaving a node, or one of its output ports."""
        ports = self._out[node]
        return ports.get(port, ()) if port is not None else tuple(e for edges in ports.values() for e in edges)

    def incoming(self, node: str, port: str | None = None) -> tuple[Edge, ...]:
        """Edges arriving at a node, or one of its input ports."""
        ports = self._in[node]
        return ports.get(port, ()) if port is not None else tuple(e for edges in ports.values() for e in edges)

    def needs(self, node: str) -> int:
        """How many required inputs must arrive before the node's first activation (0: it starts by itself)."""
        return self._needs[node]

    def team(self, node: str, *, deep: bool = True) -> tuple[str, ...]:
        """The nodes ``node`` manages: all the way down, or (``deep=False``) only those directly under it."""
        if not deep:
            return self._teams.get(node, ())
        found: list[str] = []
        below = list(self._teams.get(node, ()))
        while below:
            member = below.pop(0)
            found.append(member)
            below.extend(self._teams.get(member, ()))
        return tuple(found)

    @property
    def sources(self) -> tuple[str, ...]:
        """Nodes that wait for nothing."""
        return tuple(id for id in self.order if not self._needs[id])

    def describe(self) -> dict:
        """The definition as plain data, in a fixed order: equal graphs describe, and so hash, the same."""
        return {"nodes": sorted((n.describe() for n in self.nodes), key=lambda d: d["id"]),
                "edges": sorted((e.describe() for e in self.edges), key=lambda d: d["id"]),
                "inputs": {name: str(end) for name, end in sorted(self.inputs.items())},
                "outputs": {name: str(end) for name, end in sorted(self.outputs.items())},
                **({"manages": {manager: list(team) for manager, team in sorted(self._teams.items())}} if self._teams else {})}

    def __repr__(self):
        return f"<Graph {len(self.nodes)} nodes, {len(self.edges)} edges, {self.digest[:12]}>"


class GraphBuilder:
    def __init__(self):
        self._nodes: dict[str, Node] = {}
        self._edges: dict[str, Edge] = {}
        self._inputs: dict[str, Endpoint] = {}
        self._outputs: dict[str, Endpoint] = {}
        self._managers: dict[str, str] = {}

    @classmethod
    def of(cls, graph: "Graph") -> "GraphBuilder":
        """A builder holding everything ``graph`` has: the way to make a changed graph from an existing one."""
        builder = cls()
        builder._nodes = {node.id: node for node in graph.nodes}
        builder._edges = {edge.id: edge for edge in graph.edges}
        builder._inputs, builder._outputs = dict(graph.inputs), dict(graph.outputs)
        builder._managers = dict(graph.managers)
        return builder

    def remove(self, node: str) -> "GraphBuilder":
        """Take a node out, with every edge and graph port that touches it."""
        if node not in self._nodes:
            raise GraphError(f"no node {node!r}")
        del self._nodes[node]
        self._edges = {id: e for id, e in self._edges.items() if node not in (e.source.node, e.target.node)}
        self._inputs = {name: end for name, end in self._inputs.items() if end.node != node}
        self._outputs = {name: end for name, end in self._outputs.items() if end.node != node}
        self._managers = {member: manager for member, manager in self._managers.items() if node not in (member, manager)}
        return self

    def disconnect(self, edge: str) -> "GraphBuilder":
        if edge not in self._edges:
            raise GraphError(f"no edge {edge!r}")
        del self._edges[edge]
        return self

    def manage(self, manager: str, *members: str) -> "GraphBuilder":
        """``manager`` manages ``members``: it may pause, resume, stop and restart them, and those they manage
        in turn. A node has one manager at most."""
        for member in members:
            if member in self._managers and self._managers[member] != manager:
                raise GraphError(f"{member} is already managed by {self._managers[member]}")
            self._managers[member] = manager
        return self

    def add(self, *nodes: Node) -> "GraphBuilder":
        for node in nodes:
            if not isinstance(node, Node):
                raise GraphError(f"not a node: {node!r}")
            if node.id in self._nodes:
                raise GraphError(f"duplicate node: {node.id}")
            self._nodes[node.id] = node
        return self

    def connect(self, source, target, protocol: str = "value", *, id: str | None = None, back: bool = False,
                max_rounds: int | None = None, challenges: int | None = None) -> "GraphBuilder":
        """Join ``"node.port"`` to ``"node.port"`` under the protocol named (``value`` when none is). The id
        defaults to ``"a.out>b.in"``. ``challenges``: the receiver may reject what comes this many times."""
        try:
            source, target = Endpoint.of(source), Endpoint.of(target)
            edge = Edge(id or f"{source}>{target}", source, target, protocol, back, max_rounds, challenges)
        except EdgeError as exc:
            raise GraphError(str(exc)) from None
        if edge.id in self._edges:
            raise GraphError(f"duplicate edge: {edge.id}")
        self._edges[edge.id] = edge
        return self

    def input(self, name: str, target) -> "GraphBuilder":
        """Name an input port as an input of the graph itself: whoever runs the graph feeds it."""
        return self._expose(self._inputs, name, target)

    def output(self, name: str, source) -> "GraphBuilder":
        """Name an output port as an output of the graph itself."""
        return self._expose(self._outputs, name, source)

    def _expose(self, table, name, end) -> "GraphBuilder":
        if not isinstance(name, str) or not name or name in table:
            raise GraphError(f"invalid or duplicate graph port: {name!r}")
        try:
            table[name] = Endpoint.of(end)
        except EdgeError as exc:
            raise GraphError(str(exc)) from None
        return self

    # ---- compile ---------------------------------------------------------------------------------------

    def compile(self, protocols: Protocols | None = None) -> Graph:
        """Check the graph against ``protocols`` (the standard ones when None) and freeze it."""
        protocols = resolve(protocols)
        nodes, edges = tuple(self._nodes.values()), tuple(self._edges.values())
        if not nodes:
            raise GraphError("a graph needs at least one node")
        out_index: dict[str, dict[str, list]] = {n.id: {port: [] for port in n.outputs} for n in nodes}
        in_index: dict[str, dict[str, list]] = {n.id: {port: [] for port in n.inputs} for n in nodes}
        fed: dict[Endpoint, int] = {}           # forward feeds of each input port
        called: set[Endpoint] = set()           # input ports that are the target of a call
        for edge in edges:
            source, target = self._port(edge.source, "outputs", edge), self._port(edge.target, "inputs", edge)
            protocol = protocols.get(edge.protocol)
            if protocol is None:
                raise GraphError(f"edge {edge.id}: unknown protocol {edge.protocol!r}")
            try:
                protocol.check(source, target)
            except ProtocolError as exc:
                raise GraphError(f"edge {edge.id}: {exc}") from None
            out_index[edge.source.node][edge.source.port].append(edge)
            in_index[edge.target.node][edge.target.port].append(edge)
            if protocol.delivery is Delivery.CALL:
                called.add(edge.target)       # any number of callers may ask the same port
            elif not edge.back:
                fed[edge.target] = fed.get(edge.target, 0) + 1
        for name, end in self._inputs.items():
            self._port(end, "inputs", f"graph input {name}")
            fed[end] = fed.get(end, 0) + 1
        for name, end in self._outputs.items():
            self._port(end, "outputs", f"graph output {name}")
        needs = {}
        for node in nodes:
            count = 0
            for port in node.inputs.values():
                feeds = fed.get(Endpoint(node.id, port.name), 0)
                if feeds > 1 and not port.many:
                    raise GraphError(f"{node.id}.{port.name} takes one feed, {feeds} are connected")
                if port.required:
                    if not feeds and Endpoint(node.id, port.name) not in called:
                        raise GraphError(f"{node.id}.{port.name} is required and nothing feeds it")
                    count += 1
            if node.firing is Firing.ANY:
                if not any(fed.get(Endpoint(node.id, name)) or in_index[node.id][name] for name in node.inputs):
                    raise GraphError(f"{node.id} fires on any input and nothing feeds it")
                count = 1   # one arrival, on whichever port, is enough
            needs[node.id] = count
        order = self._order(nodes, edges)
        self._check_loops(edges, out_index)
        self._check_managers()
        freeze = lambda index: {n: MappingProxyType({p: tuple(es) for p, es in ports.items()})  # noqa: E731
                                for n, ports in index.items()}
        graph = Graph(nodes, edges, dict(self._inputs), dict(self._outputs), order, freeze(out_index), freeze(in_index), needs,
                      self._managers)
        graph.protocols = protocols
        return graph

    def _check_managers(self) -> None:
        for member, manager in self._managers.items():
            for id in (member, manager):
                if id not in self._nodes:
                    raise GraphError(f"{manager} manages {member}: no node {id!r}")
            seen: set[str] = {member}
            above: str | None = manager
            while above is not None:
                if above in seen:
                    raise GraphError(f"{member} would manage itself through {manager}")
                seen.add(above)
                above = self._managers.get(above)

    def _port(self, end: Endpoint, side: str, where):
        node = self._nodes.get(end.node)
        if node is None:
            raise GraphError(f"{where if isinstance(where, str) else 'edge ' + where.id}: no node {end.node!r}")
        port = getattr(node, side).get(end.port)
        if port is None:
            kind = "output" if side == "outputs" else "input"
            raise GraphError(f"{where if isinstance(where, str) else 'edge ' + where.id}: {end.node} has no {kind} port {end.port!r}; "
                             f"its {kind}s are {sorted(getattr(node, side)) or 'none'}")
        return port

    @staticmethod
    def _order(nodes: Iterable[Node], edges: Iterable[Edge]) -> tuple[str, ...]:
        """Upstream first, ignoring back edges; ties keep the order the nodes were added in."""
        waiting = {n.id: 0 for n in nodes}
        after: dict[str, list[str]] = {n.id: [] for n in nodes}
        for edge in edges:
            if not edge.back:
                waiting[edge.target.node] += 1
                after[edge.source.node].append(edge.target.node)
        queue = deque(id for id, count in waiting.items() if not count)
        order = []
        while queue:
            current = queue.popleft()
            order.append(current)
            for following in after[current]:
                waiting[following] -= 1
                if not waiting[following]:
                    queue.append(following)
        if len(order) != len(waiting):
            stuck = sorted(id for id, count in waiting.items() if count)
            raise GraphError(f"a loop needs a back edge: {', '.join(stuck[:8])} form a cycle of forward edges")
        return tuple(order)

    @staticmethod
    def _check_loops(edges: Iterable[Edge], out_index) -> None:
        """A back edge from A to B closes a loop only if B leads to A along forward edges."""
        for edge in edges:
            if not edge.back:
                continue
            goal, seen, stack = edge.source.node, {edge.target.node}, [edge.target.node]
            while stack and goal not in seen:
                for following in out_index[stack.pop()].values():
                    for forward in following:
                        if not forward.back and forward.target.node not in seen:
                            seen.add(forward.target.node)
                            stack.append(forward.target.node)
            if goal not in seen:
                raise GraphError(f"edge {edge.id} is marked back but closes no loop: "
                                 f"{edge.target.node} does not lead to {edge.source.node}")
