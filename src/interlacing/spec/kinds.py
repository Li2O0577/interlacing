"""How each kind of node that comes with the package is built from its description."""
from __future__ import annotations

from typing import Mapping

from ..base import ANY, Is, ListOf, Port, Record, Schema
from ..protocols import FileOf
from ..nodes import (AgentNode, Branch, Collect, Desk, FolderNode, FunctionNode, RemoteNode, GraphNode, HumanNode, IsolatedNode, Pass, ProcessNode, Roster, SinkNode,
                     SourceNode, Until, WorkerNode)

_TYPES = {t.__name__: t for t in (str, int, float, bool, list, dict, bytes, type(None))}


def schema_of(described) -> Schema:
    """The inverse of ``Schema.describe``."""
    from .catalog import SpecError
    if described is None or described == "any":
        return ANY
    if isinstance(described, str):
        try:
            return Is(*(_TYPES[name] for name in described.split("|")))
        except KeyError as exc:
            raise SpecError(f"unknown type in a schema: {exc.args[0]!r}") from None
    if isinstance(described, Mapping) and "file" in described:
        limits = described["file"] or {}
        return FileOf(*limits.get("suffixes", ()), media=limits.get("media"), max_size=limits.get("max_size"),
                      min_size=limits.get("min_size"))
    if isinstance(described, Mapping) and "list" in described:
        return ListOf(schema_of(described["list"]))
    if isinstance(described, Mapping) and "record" in described:
        return Record({name: schema_of(field) for name, field in described["record"].items()},
                      described.get("optional", ()), described.get("open", False))
    raise SpecError(f"not a schema: {described!r}")


def port_of(described, catalog=None) -> Port:
    """A port from its description, or from a bare name. Its checks are functions, named as functions are."""
    if isinstance(described, str):
        return Port(described)
    if not isinstance(described, Mapping) or not isinstance(described.get("name"), str):
        from .catalog import SpecError
        raise SpecError(f'a port is a name, or a mapping with a name like {{"name": "x", "schema": "int"}}, not {described!r}')
    checks = described.get("check") or ()
    checks = [checks] if isinstance(checks, str) else list(checks)
    if checks and catalog is None:
        from .catalog import SpecError
        raise SpecError(f"{described['name']}: a check is a function, and there is no catalog to find it in")
    return Port(described["name"], schema_of(described.get("schema")), described.get("required", True),
                described.get("many", False), described.get("call", False),
                check=tuple(catalog.function(name) for name in checks), invalid=described.get("invalid", "fail"),
                repairs=described.get("repairs"))


def ports(description: Mapping, side: str, catalog=None) -> list[Port] | None:
    given = description.get(side)
    if given is not None and (isinstance(given, (str, Mapping)) or not isinstance(given, (list, tuple))):
        from .catalog import SpecError
        raise SpecError(f'{side} is a list of ports, like ["text"], not {given!r}')
    return None if given is None else [port_of(port, catalog) for port in given]


def _first_schema(description: Mapping, side: str) -> Schema:
    given = ports(description, side)
    return given[0].schema if given else ANY


def _optional(catalog, name):
    return catalog.function(name) if name else None


def function(d, catalog, graphs):
    """A function node. Ports it does not list are read from the function, as in Python: each parameter is an
    input, the output is ``out``; a first parameter called ``ctx`` is the context, unless ``context`` says."""
    from .catalog import SpecError
    from ..base import NodeError
    f = catalog.function(d["config"]["function"])
    inputs, outputs, context = ports(d, "inputs", catalog), ports(d, "outputs", catalog), d["config"].get("context")
    if inputs is None or outputs is None or context is None:
        try:
            read = FunctionNode.of(f, d["id"])
        except NodeError as exc:
            raise SpecError(f"{exc}; list its inputs and outputs") from None
        inputs = list(read.inputs.values()) if inputs is None else inputs
        outputs = list(read.outputs.values()) if outputs is None else outputs
        context = read.context if context is None else context
    return FunctionNode(d["id"], f, inputs=inputs, outputs=outputs, firing=d.get("firing"), context=bool(context),
                        retry_attempts=d["config"].get("retry_attempts", 1),
                        retry_on=d["config"].get("retry_on", ("connection", "timeout")))


def _agent(d, catalog, area) -> dict:
    """What an agent and a worker are built with alike."""
    c = d["config"]
    outputs = ports(d, "outputs", catalog)
    memory = c.get("memory", True)
    return dict(system=c.get("system", ""), tools=catalog.tools_of(c.get("tools"), area),
                inputs=ports(d, "inputs", catalog), outputs=[p for p in outputs if not p.call] if outputs is not None else None,
                firing=d.get("firing"), memory=catalog.function(memory) if isinstance(memory, str) else memory, json=c.get("json", False),
                max_steps=c.get("max_steps", 50), max_tokens=c.get("max_tokens", 4096), files=catalog.workspace,
                repairs=c.get("repairs"), resident=c.get("resident", False), doubt=c.get("doubt"),
                **_hiring(c, catalog))


def _hiring(c, catalog) -> dict:
    """What an agent that may hire is given: how many, from which templates, and how to make them."""
    if not c.get("hire"):
        return {}
    from .catalog import SpecError
    from .load import load_node
    named = c.get("templates") or []
    if isinstance(named, Mapping):
        templates = {str(name): dict(described) for name, described in named.items()}
    else:
        missing = [name for name in named if name not in catalog.templates]
        if missing:
            raise SpecError(f"no template {', '.join(map(repr, missing))} in this catalog")
        templates = {name: catalog.templates[name] for name in named}
    return dict(hire=c["hire"], templates=templates, hire_rounds=c.get("hire_rounds", 10),
                builder=lambda described: load_node(described, catalog))


def agent(d, catalog, graphs):
    area = catalog.area(d["id"], d["config"])
    return AgentNode(d["id"], catalog.model(d["config"].get("model", "")), area=area, **_agent(d, catalog, area))


def worker(d, catalog, graphs):
    """An agent that works in the workspace. Its commands start programs, so they need what a ``process``
    node needs; ``"commands": false`` leaves it the file tools alone."""
    from .catalog import SpecError
    c = d["config"]
    area = catalog.area(d["id"], c)
    if area is None:
        raise SpecError(f"{d['id']} works in a workspace, and this catalog has none")
    commands = c.get("commands", True)
    if commands and not catalog.processes:
        raise SpecError(f"{d['id']} runs commands, and this catalog does not allow processes: allow them, or give it "
                        f"\"commands\": false")
    return WorkerNode(d["id"], catalog.model(c.get("model", "")), area, commands=commands,
                      command_timeout=c.get("command_timeout", 120.0), **_agent(d, catalog, area))


def process(d, catalog, graphs):
    from .catalog import SpecError
    if not catalog.processes:
        raise SpecError(f"{d['id']} starts a program, and this catalog does not allow processes")
    c = d["config"]
    inputs = d.get("inputs")
    node = ProcessNode(d["id"], c["command"], format=c.get("format", "text"), lines=c.get("lines", False),
                       timeout=c.get("timeout"), takes_input=inputs is None or bool(inputs), cwd=c.get("cwd"))
    node.workspace = str(catalog.workspace) if catalog.workspace is not None else None
    return node


def isolated(d, catalog, graphs):
    from .catalog import SpecError
    if not catalog.processes:
        raise SpecError(f"{d['id']} starts a process, and this catalog does not allow processes")
    c = d["config"]
    catalog.function(c["function"])                 # it must be a function the graph may name
    node = IsolatedNode(d["id"], c["function"], inputs=ports(d, "inputs", catalog) or (), outputs=ports(d, "outputs", catalog) or (),
                        firing=d.get("firing"), timeout=c.get("timeout"), cwd=c.get("cwd"))
    node.workspace = str(catalog.workspace) if catalog.workspace is not None else None
    return node


def folder(d, catalog, graphs):
    from .catalog import SpecError
    if not catalog.processes:
        raise SpecError(f"{d['id']} starts an application, and this catalog does not allow processes")
    c = d["config"]
    node = FolderNode(catalog.app(c["path"]), d["id"], timeout=c.get("timeout"), firing=d.get("firing"))
    node.given = c["path"]
    node.workspace = str(catalog.workspace) if catalog.workspace is not None else None
    return node


def remote(d, catalog, graphs):
    """A node served elsewhere. Its token is read from the environment variable ``token_env`` names, never
    from the graph file."""
    import os
    c = d["config"]
    address = catalog.remote(c["address"])
    token = os.environ.get(c["token_env"]) if c.get("token_env") else None
    node = RemoteNode(d["id"], address, token=token, firing=d.get("firing"),
                      inputs=ports(d, "inputs", catalog), outputs=ports(d, "outputs", catalog))
    node.token_env = c.get("token_env")
    return node


def source(d, catalog, graphs):
    c = d["config"]
    return SourceNode(d["id"], catalog.function(c["read"]), schema=_first_schema(d, "outputs"),
                      connect=_optional(catalog, c.get("connect")), disconnect=_optional(catalog, c.get("disconnect")))


def sink(d, catalog, graphs):
    c = d["config"]
    return SinkNode(d["id"], catalog.function(c["write"]), schema=_first_schema(d, "inputs"), answers=c.get("answers", False),
                    connect=_optional(catalog, c.get("connect")), disconnect=_optional(catalog, c.get("disconnect")))


def human(d, catalog, graphs):
    name = d["config"].get("ask")
    node = HumanNode(d["id"], catalog.function(name) if name else None,
                     request=_first_schema(d, "inputs"), answer=_first_schema(d, "outputs"))
    if not name:
        if catalog.ask_typed is None:
            node.host = catalog.ask
        else:
            answer = node.outputs["answer"].schema.describe()
            node.host = lambda where, turn, request: catalog.ask_typed(where, turn, request, answer)
    return node


def graph(d, catalog, graphs):
    from .catalog import SpecError
    from .load import load
    inner = d["config"].get("graph")
    if isinstance(inner, str):
        if inner not in graphs:
            raise SpecError(f"{d['id']}: the description has no graph {inner[:12]} under \"graphs\"")
        inner = graphs[inner]
    return GraphNode(d["id"], load({**inner, "graphs": {**graphs, **inner.get("graphs", {})}}, catalog.inside(d["id"])),
                     catalog.protocols, workers=catalog.graph_workers(d["config"]), firing=d.get("firing"))


def block(d, catalog, graphs):
    """A use of a block kept in a file of its own (or written in place): ``{"kind": "block", "file": ...}``.
    ``defaults`` given here replace the block's own, so one design serves different uses."""
    import json
    from .catalog import SpecError
    from .load import load
    c = d["config"]
    if "file" in c:
        try:
            inner = json.loads(catalog.text(c["file"]))
        except ValueError as exc:
            raise SpecError(f"{d['id']}: {c['file']} is not JSON: {exc}") from None
    elif isinstance(c.get("graph"), Mapping):
        inner = c["graph"]
    else:
        raise SpecError(f"{d['id']}: a block needs a file, or a graph written in place")
    if not isinstance(inner, Mapping):
        raise SpecError(f"{d['id']}: a block is a description of a graph")
    given, own = c.get("defaults") or {}, inner.get("defaults") or {}
    defaults = {kind: {**own.get(kind, {}), **given.get(kind, {})} for kind in own.keys() | given.keys()}
    inner = {**inner, "defaults": defaults, "graphs": {**graphs, **inner.get("graphs", {})}}
    workers = catalog.graph_workers(c)
    return GraphNode(d["id"], load(inner, catalog.inside(d["id"])), catalog.protocols, workers=workers, firing=d.get("firing"))


BUILT_IN = {
    "block": block,
    "function": function, "agent": agent, "worker": worker, "process": process, "isolated": isolated, "folder": folder, "remote": remote, "source": source, "sink": sink, "human": human,
    "graph": graph,
    "roster": lambda d, catalog, graphs: Roster(d["id"], d["config"]["size"]),
    "pass": lambda d, catalog, graphs: Pass(d["id"], schema=_first_schema(d, "inputs")),
    "collect": lambda d, catalog, graphs: Collect(d["id"], schema=_first_schema(d, "inputs")),
    "desk": lambda d, catalog, graphs: Desk(d["id"], takes=ports(d, "inputs", catalog) or ("x",), gives=ports(d, "outputs", catalog) or ("y",),
                                            sort=_optional(catalog, d["config"].get("sort")), wait=d.get("firing", "all") != "any"),
    "branch": lambda d, catalog, graphs: Branch(d["id"], catalog.function(d["config"]["condition"]), schema=_first_schema(d, "inputs")),
    "until": lambda d, catalog, graphs: Until(d["id"], catalog.function(d["config"]["condition"]), limit=d["config"].get("limit"),
                                              strict=d["config"].get("strict", False), schema=_first_schema(d, "inputs")),
}
