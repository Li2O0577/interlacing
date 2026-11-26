"""Prepare a supervisor's value correction from a settled durable root position."""
import json

from .engine.durable import payload_key
from .engine.journal import pack_body
from .spec import SpecError
from .spec.kinds import schema_of


def output_correction(graph, state, edit, *, actor):
    if edit is None:
        return None
    if not isinstance(edit, dict) or set(edit) != {"node", "token", "version", "value", "reason"}:
        raise SpecError("output requires node, token, version, value and reason")
    node, token, version, reason = (edit[k] for k in ("node", "token", "version", "reason"))
    if not all(isinstance(v, str) and v.strip() for v in (node, token, reason)) or type(version) is not int:
        raise SpecError("provide a node, message token, integer version and nonempty reason")
    message = state.rework.messages.get(node, {}).get(token)
    if not message or version != message["version"]:
        raise SpecError("message or version unavailable at this position")
    if state.data["changes"]:
        raise SpecError("output edits require a static graph at this position")
    nodes = {n["id"]: n for n in graph["nodes"]}
    port = next(p for p in nodes[node]["outputs"] if p["name"] == message["port"])
    if port.get("check") or port.get("call"):
        raise SpecError("output edits require a data port without custom checks")
    value = edit["value"]
    json.dumps(value, allow_nan=False)
    schema_of(port["schema"]).check(value)
    key = payload_key(value)
    if key == message["key"]:
        raise SpecError("the replacement matches the committed output")
    body = pack_body(value)
    edges = {e["id"]: e for e in graph["edges"]}
    sent, routes = [], []
    for edge_id, old in message["envelopes"].items():
        edge = edges[edge_id]
        if edge["protocol"] != "value" or edge.get("back"):
            raise SpecError("output edits require forward value edges")
        receiver, _, name = edge["to"].rpartition(".")
        target = nodes[receiver]
        schema_of(next(p["schema"] for p in target["inputs"] if p["name"] == name)).check(value)
        default = "tell" if target["kind"] in ("agent", "worker") else "rerun"
        route = state.correction_route(old, target.get("on_correction", default))
        if route == "ignore":
            continue
        if route == "answer":
            raise SpecError("finish the pending challenge before editing its output")
        previous = state.rework.deliveries[old]
        sent.append([edge_id, state.data["counters"][edge_id] + 1, message["round"], body, previous[4], old])
        routes.append(route)
    return {"schema_version": 1, "kind": "correct", "node": node, "token": token, "port": port["name"],
            "round": message["round"], "version": version + 1, "key": key, "noop": False, "limited": [],
            "external": {"actor": actor, "reason": reason, "previous_version": version},
            "outputs": [[name, index, body] for name, index in message["outputs"]], "routes": routes, "sent": sent}
