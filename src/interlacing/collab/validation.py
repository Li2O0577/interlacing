"""Data-only checks shared by dry-run and submission; worker catalogs resolve callables."""
import re

from ..base.node import ID
from ..nodes import GraphNode, HumanNode
from ..spec import SpecError


def _assignment_nodes(path):
    """Keep graph node ids for topology checks. The complete key still selects the exact activation."""
    if not isinstance(path, str):
        raise SpecError("assignment paths must be strings")
    parts = path.split("/")
    names = []
    for part in parts[:-1]:
        node, _, turn = part.rpartition(".")
        if not ID.fullmatch(node) or not re.fullmatch(r"[1-9][0-9]*", turn):
            raise SpecError(f"assignments[{path!r}]: expected block.round/node with a positive round")
        names.append(node)
    if not ID.fullmatch(parts[-1]):
        raise SpecError(f"assignments[{path!r}]: expected a human node id")
    return [*names, parts[-1]]


def _unknown_target(path):
    return SpecError(f"assignments[{path!r}]: path must lead to a service-managed human node in the submitted graph")


def _declared_assignment(graph, path):
    """Check literal paths without loading application code. Generated nodes are checked after compilation."""
    names = _assignment_nodes(path)
    graphs = {}
    for index, name in enumerate(names):
        graphs.update(graph.get("graphs", {}))
        members = graph.get("nodes", [])
        if graph.get("method") not in (None, "chain", "loop", "fan") or graph.get("defaults"):
            return False
        if any("times" in node or "id" not in node for node in members):
            return False
        node = next((n for n in members if n["id"] == name), None)
        if node is None:
            if graph.get("method") == "fan":  # split and join can also be human
                return False
            raise _unknown_target(path)
        config = node.get("config", {})
        if not isinstance(config, dict):
            raise SpecError(f"assignments[{path!r}]: node config must be an object")
        if index == len(names) - 1:
            if node.get("kind") != "human" or config.get("ask", node.get("ask")):
                raise _unknown_target(path)
            return True
        if "method" in node:
            graph = node
        elif node.get("kind") in ("block", "graph"):
            if config.get("defaults", node.get("defaults")) or "file" in config or "file" in node:
                return False
            inner = config.get("graph", node.get("graph"))
            graph = graphs.get(inner) if isinstance(inner, str) else inner
            if not isinstance(graph, dict):
                return False
        else:
            raise _unknown_target(path)
    return True


def check_compiled_assignments(graph, assignments):
    for path in assignments:
        scope = graph
        names = _assignment_nodes(path)
        for name in names[:-1]:
            block = scope.node(name) if name in scope else None
            if not isinstance(block, GraphNode):
                raise _unknown_target(path)
            scope = block.graph
        leaf = scope.node(names[-1]) if names[-1] in scope else None
        if not isinstance(leaf, HumanNode) or leaf.ask is not None:
            raise _unknown_target(path)


def validate_submission(graph, workers, assignments=None):
    if type(workers) is not int or not 0 <= workers <= 32:
        raise SpecError("workers must be an integer from 0 to 32")
    if not isinstance(graph, dict):
        raise SpecError("graph must be an object")

    def check(part, path):
        if not isinstance(part, dict):
            raise SpecError(f"{path}: expected a graph or node object")
        if "method" in part and part.get("protocol", "value") not in ("value", "artifact"):
            raise SpecError(f"{path}.protocol: durable delivery requires value or artifact")
        edges = part.get("edges", [])
        if not isinstance(edges, list):
            raise SpecError(f"{path}.edges: expected a list")
        for index, edge in enumerate(edges):
            where = f"{path}.edges[{index}]"
            if not isinstance(edge, dict) or not all(isinstance(edge.get(k), str) for k in ("from", "to")):
                raise SpecError(f"{where}: expected from and to endpoints")
            budget = edge.get("challenges")
            if budget is not None and (type(budget) is not int or budget < 1):
                raise SpecError(f"{where}.challenges: expected a positive integer")
            if edge.get("protocol", "value") not in ("value", "artifact"):
                raise SpecError(f"{where} ({edge['from']} -> {edge['to']}): durable delivery requires "
                                "value or artifact")
        for field in ("nodes", "children"):
            parts = part.get(field, [])
            if not isinstance(parts, list):
                raise SpecError(f"{path}.{field}: expected a list")
            for index, child in enumerate(parts):
                check(child, f"{path}.{field}[{index}]")
        for field in ("root", "member", "split", "join", "operator"):
            if isinstance(part.get(field), dict):
                check(part[field], f"{path}.{field}")
        if part.get("kind") in ("block", "graph"):
            config = part.get("config", {})
            if not isinstance(config, dict):
                raise SpecError(f"{path}.config: expected an object")
            inner = config.get("graph", part.get("graph"))
            if isinstance(inner, dict):
                check(inner, f"{path}.graph")
        graphs = part.get("graphs", {})
        if not isinstance(graphs, dict):
            raise SpecError(f"{path}.graphs: expected an object")
        for digest, inner in graphs.items():
            check(inner, f"{path}.graphs[{digest}]")

    check(graph, "graph")
    checked = ["description", "durable_edges"]
    worker_checks = ["catalog", "compiled_graph", "input_schemas"]
    if assignments is not None:
        if not isinstance(assignments, dict):
            raise SpecError("assignments must map task paths to member names")
        deferred = False
        for path, member in assignments.items():
            if not isinstance(member, str) or not member.strip():
                raise SpecError(f"assignments[{path!r}]: expected a member name")
            if not _declared_assignment(graph, path):
                deferred = True
        if deferred:
            worker_checks.append("assignment_paths")
        else:
            checked.append("assignment_paths")
    return {"workers": workers, "checked": checked, "worker_checks": worker_checks}
