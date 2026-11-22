"""A node that runs a graph it is given while the run is going on.

A graph is fixed when it is compiled. Where the work to be done is only known half way through, a
coordinator writes the graph it needs as data and sends it to a ``SpawnNode``, which builds it, runs it and
sends back what came out. Nothing about the outer graph changes: the new graph lives and dies inside one
activation of this node, like the inside of a block.

What such a graph may reach is not the sender's to say. It is built with the catalog the node was given,
so it can name only the functions, models and programs that catalog allows, and it is no larger and nests
no deeper than the node permits.
"""
from __future__ import annotations

from typing import Any, Mapping

from ..base import Graph, Node, NodeError, Port
from ..engine import RunError
from ..nodes import GraphNode
from .catalog import Catalog, SpecError


def size(graph: Graph) -> int:
    """How many nodes a graph holds, counting those inside its blocks."""
    return sum(1 + (size(node.graph) if isinstance(node, GraphNode) else 0) for node in graph.nodes)


class SpawnNode(Node):
    """Takes ``{"graph": <description>, "inputs": {...}}`` on ``request`` and sends
    ``{"outputs": {name: [values]}, "rounds": {...}, "problems": [...]}`` out of ``result``. Joined by a call
    edge it is a tool: an agent calls it with a graph and gets the outcome as the answer.

    A description that cannot be built, and a graph that fails while it runs, are answered with
    ``{"error": ...}`` so that the sender can try something else; with ``strict=True`` they fail this node
    instead.

    ``max_nodes``   the largest graph it will build, counting nodes inside blocks
    ``depth``       how many levels of graphs that spawn graphs it allows below itself (0: none)
    ``max_runs``    how many graphs it will run in one run of the outer graph (``None``: no limit)
    ``workers``     threads the spawned graph's nodes may run on
    """

    kind = "spawn"
    time_limit = float("inf")      # the nodes of the graph it runs are timed one by one

    def __init__(self, id: str, catalog: Catalog, *, max_nodes: int = 50, depth: int = 0, max_runs: int | None = None,
                 workers: int = 0, strict: bool = False):
        if not isinstance(catalog, Catalog):
            raise NodeError(f"{id}: a spawn node needs the catalog its graphs are built with")
        for name, value, least in (("max_nodes", max_nodes, 1), ("depth", depth, 0), ("workers", workers, 0)):
            if type(value) is not int or value < least:
                raise NodeError(f"{id}: {name} must be a whole number of at least {least}")
        if max_runs is not None and (type(max_runs) is not int or max_runs < 1):
            raise NodeError(f"{id}: max_runs must be a whole number of at least 1")
        super().__init__(id, inputs=[Port("request")], outputs=[Port("result")])
        self.catalog, self.max_nodes, self.depth, self.max_runs = catalog, max_nodes, depth, max_runs
        self.workers, self.strict = workers, bool(strict)

    def config(self) -> Mapping:
        return {"max_nodes": self.max_nodes, "depth": self.depth, "max_runs": self.max_runs, "workers": self.workers,
                "strict": self.strict}

    def tool(self, name: str = "grow", description: str | None = None):
        """This node as a tool for an agent, joined to it by a call edge (``agent.<name>`` to ``<id>.request``):
        its description holds the graph format and the functions this node's catalog allows, so the model
        needs to be told nothing else to write a graph."""
        from ..nodes import Tool
        from .guide import guide
        told = description or (f"Build a graph described as JSON and run it; you get its outputs back, or an error "
                               f"saying what to fix. At most {self.max_nodes} nodes.")
        return Tool(name, f"{told}\n\n{guide(self.catalog)}", {
            "type": "object", "properties": {
                "graph": {"type": "object", "description": "The graph, written as the format above says."},
                "inputs": {"type": "object", "description": 'A value for each input of the graph, e.g. {"in": ...}.'}},
            "required": ["graph", "inputs"]})

    def build(self, description: Any) -> Graph:
        """The graph a description stands for, within this node's limits."""
        if not isinstance(description, Mapping):
            raise SpecError("graph must be the description of a graph: nodes and edges, or a method and its parts")
        graph = _load(description, _below(self.catalog, self.depth, self.max_nodes, self.workers))
        if size(graph) > self.max_nodes:
            raise SpecError(f"the graph has {size(graph)} nodes, and at most {self.max_nodes} may be made here")
        return graph

    def run(self, ctx, inputs: Mapping[str, Any]):
        request = inputs["request"]
        try:
            if self.max_runs is not None and ctx.round > self.max_runs:
                raise SpecError(f"{self.max_runs} graphs have been run here already, and that is the limit")
            if not isinstance(request, Mapping):
                raise SpecError("the request must be a mapping with graph and inputs")
            given = request.get("inputs") or {}
            if not isinstance(given, Mapping):
                raise SpecError("inputs must be a mapping: a value for each input of the graph")
            graph = self.build(request.get("graph"))
            unknown = sorted(given.keys() - graph.inputs.keys())
            if unknown:
                raise SpecError(f"the graph has no input {unknown}; its inputs are {sorted(graph.inputs)}")
        except SpecError as exc:
            if self.strict:
                raise NodeError(f"the graph cannot be built: {exc}") from None
            return {"result": {"error": f"the graph cannot be built: {exc}"}}
        try:
            result = ctx.engine(graph, self.catalog.protocols, workers=self.workers, firm=True).go(given, fresh=bool(ctx.problems))
        except RunError as exc:
            if self.strict or ctx.stopping():
                raise
            return {"result": {"error": str(exc), **({"node": exc.node} if exc.node else {})}}
        return {"result": {"outputs": result.outputs, "rounds": result.rounds, "problems": result.problems,
                           **({} if result.complete else {"complete": False})}}


def _load(description: Mapping, catalog: Catalog) -> Graph:
    from .load import load
    return load(description, catalog)


def _below(catalog: Catalog, depth: int, max_nodes: int, workers: int) -> Catalog:
    """The catalog for a graph made by a spawn node that allows ``depth`` more levels below itself. What is
    made there is held to what the node above was given: no larger a graph, no more threads."""
    import copy
    inner = copy.copy(catalog)
    inner._kinds = dict(catalog._kinds)
    inner.max_workers = workers if catalog.max_workers is None else min(workers, catalog.max_workers)

    def least(asked, most):
        return min(asked, most) if type(asked) is int else asked       # what is not a number is refused further on

    def spawn(d, catalog_, graphs):
        if depth < 1:
            raise SpecError(f"{d['id']}: a graph made here may not make graphs of its own")
        c = d["config"]
        return SpawnNode(d["id"], inner, max_nodes=least(c.get("max_nodes", 50), max_nodes), depth=least(c.get("depth", 0), depth - 1),
                         max_runs=c.get("max_runs"), workers=least(c.get("workers", 0), workers), strict=c.get("strict", False))
    inner._kinds["spawn"] = spawn
    return inner


def loader(d: Mapping, catalog: Catalog, graphs: Mapping) -> SpawnNode:
    """How a ``spawn`` node written in a graph file is built: with the catalog the file itself is loaded with."""
    c = d["config"]
    return SpawnNode(d["id"], catalog, max_nodes=c.get("max_nodes", 50), depth=c.get("depth", 0), max_runs=c.get("max_runs"),
                     workers=c.get("workers", 0), strict=c.get("strict", False))
