"""How a graph is written as data, said briefly enough to be given to a model, and what a catalog lets it name.

``GUIDE`` is the format in a page. ``functions(catalog)`` lists the functions a catalog allows, each with its
signature and the first line of its documentation. ``guide(catalog)`` is both together: what an MCP client,
an agent holding a ``SpawnNode`` as a tool, or a person new to the format reads before writing a graph.
"""
from __future__ import annotations

import importlib
import inspect

from .catalog import Catalog

GUIDE = """\
A graph is JSON. Write it in one of two ways.

1. A method and its nodes; the graph's input is "in" and its output "out":
   {"method": "chain", "nodes": [A, B, C]}            A's output goes to B, B's to C
   {"method": "fan", "nodes": [A, B]}                 "in" goes to A and B side by side; "out" is the list [A's, B's]
   {"method": "loop", "nodes": [A, B], "max_rounds": 3, "until": "mod.done"}
   A node of a method may itself be a method, written in place:
   {"method": "chain", "nodes": [A, {"method": "fan", "nodes": [B, C]}, D]}   D gets [B's, C's]

2. Nodes and edges, for any shape:
   {"nodes": [A, B],
    "edges": [{"from": "a.out", "to": "b.text"}],     "node.port" to "node.port"
    "inputs": {"text": "a.text"},                     graph input name -> "node.port" it feeds
    "outputs": {"result": "b.out"}}                   graph output name -> "node.port" it comes from

A node is a mapping with an id and a kind:
   {"id": "clean", "kind": "function", "function": "mymod.clean"}
     a function node's inputs are the function's parameters and its output is "out", unless you list them:
     "inputs": ["text"], "outputs": ["out"]. A first parameter named ctx is the run's context, not an input.
   {"id": "lead", "kind": "agent", "model": "...", "system": "You are ..."}   inputs ["task"], output "out"
   {"id": "sub", "kind": "graph", "graph": {...}}                            a whole graph used as one node
Ports are names ("text") or mappings ({"name": "text", "schema": {"type": "string"}}).
"defaults": {"agent": {"model": "..."}} at the top gives settings to every node of that kind.
"times": 3 on a node makes three copies, "<id>-1" .. "<id>-3".
A setting may come from a file beside the graph: "system": {"file": "prompts/lead.md"}.
A function may be named only if it is among the allowed functions below.

Running it: give a value for each graph input, e.g. {"in": "some text"} for a method, {"text": ...} for the
graph in 2. The result holds each output's values as a list.
"""


def functions(catalog: Catalog) -> list[dict]:
    """The functions a graph built with this catalog may name: those defined at the top of each allowed module
    that can be imported, with their signatures and the first line of their documentation."""
    found: list[dict] = []
    for module_name in catalog.allow:
        try:
            module = importlib.import_module(module_name)
        except ImportError:
            continue
        for name, value in sorted(vars(module).items()):
            if name.startswith("_") or not callable(value) or inspect.isclass(value) or inspect.ismodule(value):
                continue
            dotted = f"{module_name}.{name}"
            try:
                catalog.function(dotted)
            except ValueError:            # imported from elsewhere, or otherwise not one a graph may name
                continue
            inner = getattr(value, "function", None) or getattr(value, "__wrapped__", None) or value
            try:
                signature = str(inspect.signature(inner))
            except (TypeError, ValueError):
                signature = "(...)"
            doc = (inspect.getdoc(inner) or "").strip().splitlines()
            found.append({"name": dotted, "signature": signature, **({"doc": doc[0]} if doc else {})})
    return found


def guide(catalog: Catalog | None = None) -> str:
    """``GUIDE``, followed by the functions the catalog allows."""
    listed = functions(catalog) if catalog is not None else []
    if not listed:
        return GUIDE + "\nAllowed functions: none.\n"
    lines = [f"- {f['name']}{f['signature']}" + (f"  {f['doc']}" if f.get("doc") else "") for f in listed]
    return GUIDE + "\nAllowed functions:\n" + "\n".join(lines) + "\n"
