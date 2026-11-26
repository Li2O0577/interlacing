"""Explain recorded dependencies without loading node code or repairing a live journal."""
from __future__ import annotations

import json
from collections import defaultdict, deque
from collections.abc import Iterable
from pathlib import Path

from .engine.journal import Journal, JournalError, history_record
from .engine.wiring import INPUT


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
    """Explain a run from a JSONL journal path."""
    return explain_records(graph, Journal(path).inspect(), status, questions, timeline=timeline)


def _provenance(history: list[dict]) -> dict:
    """Index recorded reads and output versions within a single journal scope."""
    start = next((event for event in history if event["kind"] == "run"), {})
    delivery_sources: dict[str, list[dict]] = defaultdict(list)
    activations = []
    outputs: dict[str, list[list[dict]]] = defaultdict(list)
    output_counts: dict[str, int] = defaultdict(int)
    children: dict[tuple, tuple[int, list[dict]]] = {}
    for event in history:
        seq = event["seq"]
        output_messages: dict[tuple, deque] = defaultdict(deque)
        delivery_messages: dict[str, list[dict]] = defaultdict(list)
        for ordinal, (token, _, envelopes, slots) in enumerate(event.get("emits", ()), 1):
            message = {"token": token, "ordinal": ordinal}
            if "message_version" in event:
                message["version"] = event["message_version"]
            for _, delivery in envelopes:
                delivery_messages[delivery].append(message)
            for name, index in slots:
                output_messages[name, index].append(message)
        for delivery, replaces in event.get("deliveries", ()):
            messages = delivery_messages[delivery]
            delivery_sources[delivery].append({"event_seq": seq, "replaces": replaces,
                                                "message": messages[0] if len(messages) == 1 else None})
        if event["kind"] == "act":
            activations.append({"event_seq": seq, "node": event["node"], "round": event["round"],
                                "reads": event["reads"], **({"stale": True} if event.get("stale") else {})})
        written = []
        indices = event.get("output_indices")
        for position, name in enumerate(event.get("outputs", ())):
            index = indices[position] if indices is not None else output_counts[name]
            output_counts[name] = max(output_counts[name], index + 1)
            written.append((name, index))
        for name, index in [*written, *event.get("revised_outputs", ())]:
            # A legacy or changed graph can have revisions to slots whose origin is absent.
            while index >= len(outputs[name]):
                outputs[name].append([])
            versions = outputs[name][index]
            slot_messages = output_messages[name, index]
            versions.append({"event_seq": seq, "message": slot_messages.popleft() if slot_messages else None,
                             "replaces_event_seq": versions[-1]["event_seq"] if versions else None})
        for index, child in enumerate(event.get("child_history", ())):
            scope = next((entry["scope_id"] for entry in child if entry["kind"] == "run"), None)
            key = ("scope", scope) if scope else ("position", seq, index)
            root_seq = children[key][0] if key in children else seq
            children[key] = root_seq, child

    # A consumer can finish journaling before the sender. Resolve against the whole viewed prefix.
    for activation in activations:
        reads = activation["reads"]
        if reads is None:
            continue
        resolved = []
        for delivery in reads:
            sources = delivery_sources.get(delivery, [])
            read = {"delivery": delivery, "source": sources[0] if len(sources) == 1 else None}
            if len(sources) != 1:
                read["status"] = "ambiguous" if sources else "not_recorded"
            resolved.append(read)
        activation["reads"] = resolved
    return {"run_id": start.get("run_id"), "scope_id": start.get("scope_id"),
            "activations": activations, "outputs": dict(outputs),
            "children": [{"parent_event_seq": seq, **_provenance(child)} for seq, child in children.values()]}


def explain_records(graph: dict, records: Iterable[dict], status: str, questions: list, *, timeline: bool = False) -> dict:
    """First-activation dependencies in the saved graph, with historical delivery evidence."""
    sent: set[str] = set()
    activated: set[str] = set()
    changed = False
    unknown = False
    descriptions = {n["id"]: dict(n) for n in graph["nodes"]}
    edges = {e["id"]: e for e in graph["edges"]}
    inputs_map, outputs = dict(graph["inputs"]), dict(graph["outputs"])
    managers = {member: manager for manager, team in graph.get("manages", {}).items() for member in team}
    events, history = [], []
    for seq, record in enumerate(records, 1):
        kind = record["kind"]
        if timeline:
            history.append(history_record(record, seq))
            event = {"seq": seq, "schema_version": history[-1]["schema_version"]}
            event.update({key: record[key] for key in ("kind", "node", "round", "open", "elapsed_seconds", "amounts",
                                                       "action", "nodes", "by", "edge", "answer", "reason", "problems", "stale")
                          if key in record})
            if kind in ("challenge", "review_reply", "correct"):
                event.update({k: history[-1][k] for k in ("id", "envelope", "message", "version", "replies")
                              if k in history[-1]})
            if kind in ("challenged", "verdict"):
                event["id"] = record["id"]
            if record.get("fixed_out"):
                event["revised_outputs"] = [{"output": name, "index": index}
                                            for name, index, _ in record["fixed_out"]]
            if kind == "bind":
                event["took"] = record["took"]
                if "redo" in record:
                    event["reworks_round"] = int(record["redo"])
            if kind == "correct":
                event.update(token=record["token"], version=record["version"], noop=record["noop"])
                event["revised_outputs"] = [{"output": name, "index": index} for name, index, _ in record["outputs"]]
            if kind == "fork":
                event["origin"] = record["origin"]
                event["inputs"] = [item[0].removeprefix(INPUT) for item in record["sent"]]
            if kind == "topology":
                event["operations"] = [op[0] for op in record["operations"]]
            elif record.get("changes"):
                event["operations"] = [op[0] for ops in record["changes"] for op in ops]
            events.append(event)
        changes = (record["operations"],) if kind == "topology" else record.get("changes", ())
        for ops in changes:
            if unknown:
                break
            _apply(ops, descriptions, edges, inputs_map, outputs, sent, activated, managers)
            changed = True
        if kind in ("run", "fork", "act", "deliver", "correct"):
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
            "managers": managers, **({"timeline": events, "provenance": _provenance(history)} if timeline else {})}


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
