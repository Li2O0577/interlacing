"""Explain recorded dependencies without loading node code or repairing a live journal."""
from __future__ import annotations

import json
import os
from collections.abc import Iterator
from collections import defaultdict
from pathlib import Path

from .engine.journal import JournalError
from .engine.wiring import INPUT


def _records(path: Path) -> Iterator[dict]:
    try:
        file = path.open("rb")
    except FileNotFoundError:
        return
    with file:
        # A producer can still be appending. Inspect only the complete prefix present at open.
        remaining = os.fstat(file.fileno()).st_size
        number = 0
        while remaining:
            line = file.readline(remaining)
            if not line.endswith(b"\n"):
                return
            remaining -= len(line)
            number += 1
            try:
                yield json.loads(line)
            except ValueError as error:
                raise JournalError(f"{path}: line {number} is damaged: {error}") from None


def _apply(ops, nodes, edges, inputs, outputs, sent, activated, managers):
    """Project committed Change operations onto plain descriptions without invoking node factories."""
    for kind, *args in ops:
        first = args[0]
        if kind == "add":
            nodes[first["id"]] = dict(first)
        elif kind == "remove":
            nodes.pop(first, None)
            activated.discard(first)
            prefix = first + "."
            for key, edge in list(edges.items()):
                if edge["from"].startswith(prefix) or edge["to"].startswith(prefix):
                    del edges[key]
                    sent.discard(key)
            for table in (inputs, outputs):
                for key, end in list(table.items()):
                    if end.startswith(prefix):
                        del table[key]
            for member, manager in list(managers.items()):
                if first in (member, manager):
                    del managers[member]
        elif kind == "disconnect":
            edges.pop(first, None)
            sent.discard(first)
        elif kind == "connect":
            edges[first["id"]] = first
        elif kind == "ports":
            for key, added in zip(("inputs", "outputs"), args[1:]):
                nodes[first][key] = [*nodes[first][key], *added]
        elif kind == "output":
            outputs[first] = args[1]
        elif kind == "manage":
            managers.update((member, first) for member in args[1])
        else:
            raise JournalError(f"unknown graph change: {kind}")


def explain(graph: dict, path: Path, status: str, questions: list, *, timeline: bool = False) -> dict:
    """First-activation dependencies in the saved graph, with historical delivery evidence."""
    sent: set[str] = set()
    activated: set[str] = set()
    changed = False
    unknown = False
    descriptions = {n["id"]: dict(n) for n in graph["nodes"]}
    edges = {e["id"]: e for e in graph["edges"]}
    inputs_map, outputs = dict(graph["inputs"]), dict(graph["outputs"])
    managers = {member: manager for manager, team in graph.get("manages", {}).items() for member in team}
    events = []
    for record in _records(path):
        kind = record["kind"]
        if timeline:
            event = {key: record[key] for key in ("kind", "node", "round", "open", "elapsed_seconds", "amounts",
                                                   "action", "nodes", "by", "edge", "answer", "reason", "problems")
                     if key in record}
            if kind in ("challenged", "verdict"):
                event["id"] = record["id"]
            if record.get("changes"):
                event["operations"] = [op[0] for ops in record["changes"] for op in ops]
            events.append(event)
        for ops in record.get("changes", ()):
            if unknown:
                break
            _apply(ops, descriptions, edges, inputs_map, outputs, sent, activated, managers)
            changed = True
        if kind in ("run", "act"):
            sent.update(envelope[0] for envelope in record.get("sent", ()))
        if kind == "act":
            activated.add(record["node"])
            activated.update(record.get("calls", ()))
        if kind == "change":
            unknown = True  # resume(change=True) records a digest rather than a full definition

    incoming: dict[str, list] = defaultdict(list)
    for edge in edges.values():
        incoming[edge["to"]].append(edge)
    for name, target in inputs_map.items():
        incoming[target].append({"id": INPUT + name, "from": INPUT + name, "protocol": "value"})

    pending = defaultdict(list)
    for question in questions:
        pending[question["node"]].append(question["id"])
    nodes = {}
    for node in descriptions.values():
        name = node["id"]
        inputs = {}
        missing: list[str] = []
        for port in node["inputs"]:
            target = f"{name}.{port['name']}"
            feeds = incoming[target]
            inputs[port["name"]] = [
                {"from": edge["from"], "protocol": edge["protocol"], "delivery_recorded": edge["id"] in sent}
                for edge in feeds
            ]
            if port["required"] and all(not e.get("back") and e["protocol"] in ("value", "signal", "latest", "file", "artifact")
                                        for e in feeds):
                missing.extend(e["from"] for e in feeds if e["id"] not in sent)
        info: dict = {"inputs": inputs, "activation_recorded": name in activated}
        if pending[name]:
            info["questions"] = pending[name]
            info["reason"] = "Awaiting a human answer to " + ", ".join(pending[name])
        elif (status == "waiting" and not unknown and node["firing"] == "all"
              and name not in activated and missing):
            info["awaiting_first_delivery_from"] = missing
            info["reason"] = "No first delivery recorded from " + ", ".join(missing)
        nodes[name] = info
    topology = "journaled graph" if changed else "saved graph"
    if unknown:
        topology = "last known graph; external graph revision cannot be reconstructed"
    return {"status": status, "topology": topology,
            "evidence": "recorded deliveries, not live mailbox contents", "nodes": nodes, "outputs": outputs,
            "managers": managers, **({"timeline": events} if timeline else {})}


def diagram(view: dict, format: str) -> str:
    """Generate stable diagram identifiers independently of user node names."""
    dot = format == "dot"
    lines = ["digraph run {" if dot else "flowchart TD"]
    if view["topology"].startswith("last known"):
        lines.append(("  // " if dot else "  %% ") + view["topology"])
    ids = {name: f"n{i}" for i, name in enumerate(view["nodes"])}
    for name, ident in ids.items():
        label = json.dumps(name)
        lines.append(f"  {ident} [label={label}];" if dot else f"  {ident}[{label}]")
    for name, node in view["nodes"].items():
        for port, feeds in node["inputs"].items():
            for feed in feeds:
                source, _, out = feed["from"].partition(".")
                if source not in ids:
                    continue
                label = json.dumps(f"{out} to {port} ({feed['protocol']})")
                a, b = ids[source], ids[name]
                lines.append(f"  {a} -> {b} [label={label}];" if dot else f"  {a} -->|{label}| {b}")
    if dot:
        lines.append("}")
    return "\n".join(lines)
