"""interlacing: nodes, edges and methods, and an engine that runs the graphs they make.

A node is anything that takes information in and gives information out: a model, a function, an external
program, a device, a person, another graph. An edge carries information from one node to another under a
written protocol. A method is a reusable way of joining nodes: a chain, a loop, a fan, a tree.

Layers, bottom up (each imports only from below):

1. base        Node, Port, Schema, Edge, Protocol, Envelope, Graph, GraphBuilder, Method
2. engine      Engine, Result, Journal, Transport
3. parts       nodes, protocols, methods, models: the ones that come with the package
   blocks      a designed graph kept as a unit and used as often as needed: Block, seq, par
4. spec        graphs written as data: dump, load, and the Catalog that says what data may reach
5. runs, cli, serve   a run kept in a directory, the ``interlacing`` command that works on one, and the
               same operations offered to an MCP client
   quick       ``run``: build and run in one step

Public names are importable from the package root. Standard library only.
"""

__version__ = "0.16.2"

_PUBLIC = {
    # base
    "Node": "base", "Port": "base", "Firing": "base", "Context": "base", "Schema": "base", "Edge": "base",
    "Invalid": "base", "Protocol": "base", "Protocols": "base", "Delivery": "base", "Envelope": "base", "Ref": "base",
    "Graph": "base", "GraphBuilder": "base", "Change": "base", "GraphError": "base", "NodeError": "base", "Method": "base",
    # engine
    "Engine": "engine", "Result": "engine", "RunError": "engine", "OverBudget": "engine", "Journal": "engine", "Transport": "engine",
    "SQLiteJournal": "engine",
    # parts
    "FunctionNode": "nodes", "AgentNode": "nodes", "Tool": "nodes", "ProcessNode": "nodes", "SourceNode": "nodes",
    "SinkNode": "nodes", "HumanNode": "nodes", "GraphNode": "nodes", "Desk": "nodes", "IsolatedNode": "nodes",
    "WorkerNode": "nodes", "FolderNode": "nodes", "RemoteNode": "nodes", "serve_node": "nodes", "time_left": "nodes", "Workspace": "nodes", "Area": "nodes", "ToolPool": "nodes",
    "Chain": "methods", "Loop": "methods", "Fan": "methods", "Tree": "methods", "Star": "methods",
    "Governed": "methods", "Pool": "methods",
    "Block": "blocks", "seq": "blocks", "par": "blocks",
    "DEFAULT": "protocols", "standard": "protocols", "File": "protocols", "FileOf": "protocols",
    "with_files": "protocols",
    "Provider": "models", "FakeProvider": "models", "ChatResult": "models",
    # graphs as data
    "node": "nodes", "as_node": "nodes", "run": "quick",
    "Catalog": "spec", "SpecError": "spec", "SpawnNode": "spec", "dump": "spec", "load": "spec",
}


def _standard():
    from .protocols import DEFAULT
    return DEFAULT


from .base import set_standard as _set_standard  # noqa: E402 - the standard protocols are named once, here

_set_standard(_standard)


def __getattr__(name):
    if name in _PUBLIC:
        import importlib
        return getattr(importlib.import_module(f"{__name__}.{_PUBLIC[name]}"), name)
    raise AttributeError(name)


__all__ = ["__version__", *_PUBLIC]
