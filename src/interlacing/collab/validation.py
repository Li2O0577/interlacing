"""Data-only checks shared by dry-run and submission; worker catalogs resolve callables."""
from ..spec import SpecError


def validate_submission(graph, workers):
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
            if edge.get("protocol", "value") not in ("value", "artifact") or edge.get("challenges"):
                raise SpecError(f"{where} ({edge['from']} -> {edge['to']}): durable delivery requires "
                                "value or artifact without challenges")
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
    return {"workers": workers, "checked": ["description", "durable_edges"],
            "worker_checks": ["catalog", "compiled_graph", "input_schemas"]}
