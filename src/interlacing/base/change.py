"""A change to a running graph: the one way a graph changes while it runs.

A node asks for one with ``ctx.change(Change().add(node).connect("a.y", node.id + ".x"))``. The engine applies
it to the graph as it stands, checks the result by every rule a graph is built by, and either refuses it (the
graph stays as it was and the node is told why) or makes it real at once. With a journal the change is kept
with the activation that asked for it, as plain data, and is made again when the run is taken up.
"""
from __future__ import annotations

from typing import Callable, Iterable

from .edge import Edge, EdgeError, Endpoint
from .graph import GraphBuilder, GraphError
from .node import Node
from .port import Port
from .schema import ANY


class Change:
    """What to do to the graph, in order. Each method returns the change, so calls can be chained."""

    __slots__ = ("ops", "grown")

    def __init__(self):
        self.ops: list[tuple] = []
        self.grown: list[tuple] = []      # (node, port names) given while this change was applied

    def ports(self, node: str, inputs: Iterable = (), outputs: Iterable = ()) -> "Change":
        """Give an existing node more ports: names, or Ports that take any value (an input given by name is
        optional). The node learns of them through ``Node.add_ports``."""
        def plain(port, required: bool) -> Port:
            port = port if isinstance(port, Port) else Port(str(port), required=required)
            if port.schema is not ANY or port.check or port.call:
                raise GraphError(f"{node}.{port.name}: a port added while the run goes on takes any value, unchecked")
            return port
        self.ops.append(("ports", str(node), tuple(plain(p, False) for p in inputs),
                         tuple(plain(p, True) for p in outputs)))
        return self

    def manage(self, manager: str, *members: str) -> "Change":
        """``manager`` manages ``members`` from now on (see ``GraphBuilder.manage``)."""
        self.ops.append(("manage", str(manager), tuple(str(member) for member in members)))
        return self

    def undo(self) -> None:
        """Take back the ports this change gave, when the graph it made was refused."""
        for node, names in reversed(self.grown):
            node.drop_ports(names)
        self.grown = []

    def add(self, *nodes: Node) -> "Change":
        for node in nodes:
            if not isinstance(node, Node):
                raise GraphError(f"not a node: {node!r}")
            self.ops.append(("add", node))
        return self

    def remove(self, *nodes: str) -> "Change":
        """Take nodes out, with every edge and graph output that touches them."""
        self.ops.extend(("remove", str(node)) for node in nodes)
        return self

    def connect(self, source, target, protocol: str = "value", *, id: str | None = None, back: bool = False,
                max_rounds: int | None = None, challenges: int | None = None) -> "Change":
        try:
            source, target = Endpoint.of(source), Endpoint.of(target)
            self.ops.append(("connect", Edge(id or f"{source}>{target}", source, target, protocol, back, max_rounds,
                                             challenges)))
        except EdgeError as exc:
            raise GraphError(str(exc)) from None
        return self

    def disconnect(self, *edges: str) -> "Change":
        self.ops.extend(("disconnect", str(edge)) for edge in edges)
        return self

    def output(self, name: str, source) -> "Change":
        """Name an output port as a new output of the graph itself."""
        try:
            self.ops.append(("output", str(name), Endpoint.of(source)))
        except EdgeError as exc:
            raise GraphError(str(exc)) from None
        return self

    def __bool__(self) -> bool:
        return bool(self.ops)

    @property
    def added(self) -> list[Node]:
        return [op[1] for op in self.ops if op[0] == "add"]

    @property
    def removed(self) -> list[str]:
        return [op[1] for op in self.ops if op[0] == "remove"]

    def apply(self, builder: GraphBuilder) -> GraphBuilder:
        """Make the change on ``builder``; ``GraphError`` says what cannot be done."""
        for op in self.ops:
            kind = op[0]
            if kind == "add":
                builder.add(op[1])
            elif kind == "remove":
                builder.remove(op[1])
            elif kind == "connect":
                edge = op[1]
                builder.connect(edge.source, edge.target, edge.protocol, id=edge.id, back=edge.back,
                                max_rounds=edge.max_rounds, challenges=edge.challenges)
            elif kind == "disconnect":
                builder.disconnect(op[1])
            elif kind == "ports":
                node = builder._nodes.get(op[1])
                if node is None:
                    raise GraphError(f"no node {op[1]!r} to give ports to")
                taken = [p.name for p in (*op[2], *op[3]) if p.name in node.inputs or p.name in node.outputs]
                if taken:
                    raise GraphError(f"{op[1]} already has the ports {', '.join(taken)}")
                node.add_ports(op[2], op[3])
                self.grown.append((node, [p.name for p in (*op[2], *op[3])]))
            elif kind == "manage":
                builder.manage(op[1], *op[2])
            else:
                builder.output(op[1], op[2])
        return builder

    def describe(self) -> list:
        """The change as plain data: an added node is what its ``describe`` gives."""
        out: list = []
        for op in self.ops:
            kind = op[0]
            if kind == "add":
                made_by = getattr(op[1], "made_by", None)     # who made it, and from what: enough to make it again
                out.append(["add", {**op[1].describe(), **({"made_by": dict(made_by)} if made_by else {})}])
            elif kind == "connect":
                out.append(["connect", op[1].describe()])
            elif kind == "output":
                out.append(["output", op[1], str(op[2])])
            elif kind == "ports":
                out.append(["ports", op[1], *([{"name": p.name, "required": p.required, "many": p.many} for p in ports]
                                              for ports in op[2:])])
            elif kind == "manage":
                out.append(["manage", op[1], list(op[2])])
            else:
                out.append([kind, op[1]])
        return out

    @classmethod
    def of(cls, described: Iterable, rebuild: Callable[[dict], Node] | None) -> "Change":
        """The change ``describe`` gave, with each added node made again by ``rebuild(description)``."""
        change = cls()
        for op in described:
            kind = op[0]
            if kind == "add":
                if rebuild is None:
                    raise GraphError(f"node {op[1].get('id')!r} was added while the run went on, and nothing was "
                                     f"given that can make it again (rebuild)")
                change.add(rebuild(op[1]))
            elif kind == "connect":
                e = op[1]
                change.connect(e["from"], e["to"], e["protocol"], id=e["id"], back=e.get("back", False),
                               max_rounds=e.get("max_rounds"), challenges=e.get("challenges"))
            elif kind == "output":
                change.output(op[1], op[2])
            elif kind == "remove":
                change.remove(op[1])
            elif kind == "disconnect":
                change.disconnect(op[1])
            elif kind == "ports":
                made = [[Port(p["name"], required=p.get("required", True), many=p.get("many", False)) for p in ports]
                        for ports in op[2:4]]
                change.ports(op[1], *made)
            elif kind == "manage":
                change.manage(op[1], *op[2])
            else:
                raise GraphError(f"unknown change: {kind!r}")
        return change


