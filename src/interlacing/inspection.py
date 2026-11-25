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


def explain(graph: dict, path: Path, status: str, questions: list) -> dict:
    """First-activation dependencies in the saved graph, with historical delivery evidence."""
    sent: set[str] = set()
    activated: set[str] = set()
    changed = False
    for record in _records(path):
        kind = record["kind"]
        if kind in ("run", "act"):
            sent.update(envelope[0] for envelope in record.get("sent", ()))
        if kind == "act":
            activated.add(record["node"])
            activated.update(record.get("calls", ()))
        if kind == "change" or record.get("changes"):
            changed = True

    incoming: dict[str, list] = defaultdict(list)
    for edge in graph["edges"]:
        incoming[edge["to"]].append(edge)
    for name, target in graph["inputs"].items():
        incoming[target].append({"id": INPUT + name, "from": INPUT + name, "protocol": "value"})

    pending = defaultdict(list)
    for question in questions:
        pending[question["node"]].append(question["id"])
    nodes = {}
    for node in graph["nodes"]:
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
        elif (status == "waiting" and not changed and node["firing"] == "all"
              and name not in activated and missing):
            info["awaiting_first_delivery_from"] = missing
            info["reason"] = "No first delivery recorded from " + ", ".join(missing)
        nodes[name] = info
    return {"status": status, "topology": "initial graph; journal records changes" if changed else "saved graph",
            "evidence": "recorded deliveries, not live mailbox contents", "nodes": nodes, "outputs": graph["outputs"]}
