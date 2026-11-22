"""The command line.

    interlacing guide  [--allow MODULE]...
    interlacing check  graph.json [--allow MODULE]... [--processes] [--tools DIRECTORY] [--apps DIRECTORY] [--remote ADDRESS]...
    interlacing run    graph.json --dir RUN [--input NAME=VALUE]... [--allow MODULE]... [--processes] [--tools DIRECTORY] [--workers N]
                         [--time-limit SECONDS] [--workspace DIRECTORY] [--apps DIRECTORY] [--remote ADDRESS]...
    interlacing resume --dir RUN
    interlacing answer --dir RUN [QUESTION] (--json VALUE | --text TEXT)
    interlacing status --dir RUN
    interlacing control (pause|resume|stop|restart) NODE... --dir RUN [--team]
    interlacing files  --dir RUN
    interlacing serve  --runs DIRECTORY [--allow MODULE]... [--processes] [--workers N] [--time-limit SECONDS]
                         [--files DIRECTORY] [--tools DIRECTORY] [--apps DIRECTORY] [--remote ADDRESS]...
    interlacing node-serve MODULE:NAME [--listen HOST:PORT] [--make] [--token-env NAME]

``guide`` prints how a graph is written and the functions the modules ``--allow`` names may give it.
``--tools`` names a tool pool: a directory of tool files the graph's agents may be given by path. ``--apps``
names a directory whose folders may be nodes, each an application with a ``node.json``. ``--remote`` names
an address of a served node the graph may reach. ``node-serve`` serves one node for graphs elsewhere: it
prints ``{"address": ...}`` and serves until interrupted; listening beyond this machine needs a token in the
environment variable ``--token-env`` names. ``files``
lists what is in a run's workspace and which node wrote each file. ``control`` sets the switch of nodes of a
run that is not being run now; it holds when the run is resumed.

A graph file may take text (a prompt, say) from files in its own directory: ``{"file": "prompts/lead.md"}``.

Every command but ``serve`` and ``guide`` prints JSON (``node-serve`` one line, then serves); ``serve`` is the MCP server in ``interlacing.serve``. The exit
status says how a run stands: 0 done, 1 failed or refused, 3 waiting for a person, 4 stopped.
"""
from __future__ import annotations

import argparse
import errno
import json
import sys
import threading
from pathlib import Path
from typing import Any

from . import __version__
from .engine import JournalError
from .runs import DONE, STOPPED, WAITING, Run, check
from .spec import SpecError

EXIT = {DONE: 0, WAITING: 3, STOPPED: 4}


def _value(text: str):
    try:
        return json.loads(text)
    except ValueError:
        return text


def _limit(text: str) -> tuple:
    name, sep, number = text.partition("=")
    try:
        if not sep or not name:
            raise ValueError
        return name, float(number) if "." in number or "e" in number.lower() else int(number)
    except ValueError:
        raise argparse.ArgumentTypeError(f"{text!r} is not NAME=NUMBER") from None


def _inputs(args) -> dict:
    inputs = {}
    if args.inputs:
        inputs.update(json.loads(Path(args.inputs).read_text(encoding="utf-8")))
    for pair in args.input or ():
        name, equals, value = pair.partition("=")
        if not equals:
            raise SpecError(f"--input takes NAME=VALUE, not {pair!r}")
        inputs[name] = _value(value)
    return inputs


def _description(path: str) -> dict:
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise SpecError(f"no such file: {path}") from None
    except ValueError as exc:
        raise SpecError(f"{path}: not JSON: {exc}") from None


def _beside(path: str) -> Path:
    return Path(path).resolve().parent


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="interlacing", description="Check and run graphs written as data. Start with: interlacing guide --allow MODULE",
        epilog="run and resume exit with 0 when the run is done, 1 when it failed or was refused, 3 when it waits for a "
               "person (answer, then resume), 4 when it stopped.")
    parser.add_argument("--version", action="version", version=__version__)
    commands = parser.add_subparsers(dest="command", required=True)

    def reach(command):
        command.add_argument("--allow", action="append", default=[], metavar="MODULE",
                             help="a module the graph's functions may come from (repeatable)")
        command.add_argument("--processes", action="store_true", help="let the graph start external programs")
        command.add_argument("--tools", metavar="DIRECTORY", help="a tool pool: tool files the graph's agents may use")
        command.add_argument("--apps", metavar="DIRECTORY", help="a directory whose folders may be nodes (needs --processes)")
        command.add_argument("--remote", action="append", default=[], metavar="ADDRESS",
                             help="tcp://host:port of a served node the graph may reach (repeatable)")

    guide = commands.add_parser("guide", help="say how a graph is written, and which functions it may name")
    guide.add_argument("--allow", action="append", default=[], metavar="MODULE", help="a module whose functions to list (repeatable)")
    check = commands.add_parser("check", help="build the graph and describe it, without running anything")
    check.add_argument("graph")
    reach(check)
    run = commands.add_parser("run", help="start a run in a new directory")
    run.add_argument("graph")
    run.add_argument("--dir", required=True, help="the directory that will hold the run")
    run.add_argument("--input", action="append", metavar="NAME=VALUE", help="a graph input; VALUE is JSON, or else text")
    run.add_argument("--inputs", metavar="FILE", help="a JSON file of graph inputs")
    run.add_argument("--workers", type=int, default=0, help="threads that run nodes side by side (default: none)")
    run.add_argument("--time-limit", type=float, metavar="SECONDS", help="the longest one activation of a node may take")
    run.add_argument("--workspace", metavar="DIRECTORY", help="where the nodes work and send file paths relative to "
                                                              "(default: files in the run's directory)")
    reach(run)
    budget: dict = dict(action="append", default=[], metavar="NAME=NUMBER", type=_limit,
                  help="the most the run may use of something its nodes report using, such as tokens=200000")
    run.add_argument("--budget", **budget)
    resume = commands.add_parser("resume", help="carry on a run that stopped, failed or was waiting",
                                 description="Carry on a run. It keeps what the run was started with: the modules it may "
                                             "use (--allow), --processes and the rest are not given again.")
    resume.add_argument("--dir", required=True)
    resume.add_argument("--budget", **budget)
    commands.add_parser("status", help="say where a run stands").add_argument("--dir", required=True)
    control = commands.add_parser("control", help="pause, resume, stop or restart nodes of a run")
    control.add_argument("action", choices=["pause", "resume", "stop", "restart"])
    control.add_argument("nodes", nargs="+", metavar="NODE")
    control.add_argument("--dir", required=True)
    control.add_argument("--team", action="store_true", help="and every node they manage, all the way down")
    commands.add_parser("files", help="list the files in a run's workspace and who wrote them").add_argument("--dir", required=True)
    answer = commands.add_parser("answer", help="answer a question a person in the graph was asked")
    answer.add_argument("--dir", required=True)
    answer.add_argument("question", nargs="?", help="the question's id; not needed when only one is open")
    given = answer.add_mutually_exclusive_group(required=True)
    given.add_argument("--json", metavar="VALUE", help="the answer, as JSON")
    given.add_argument("--text", help="the answer, as text")
    serve = commands.add_parser("serve", help="offer these operations to an MCP client over stdio")
    serve.add_argument("--runs", required=True, help="the directory under which the server keeps its runs")
    serve.add_argument("--workers", type=int, default=0, help="threads that run nodes side by side (default: none)")
    serve.add_argument("--time-limit", type=float, metavar="SECONDS", help="the longest one activation of a node may take")
    serve.add_argument("--files", metavar="DIRECTORY", help="a directory graphs may take text from (default: none)")
    serve.add_argument("--budget", **budget)
    reach(serve)
    served = commands.add_parser("node-serve", help="serve one node, for graphs elsewhere to use as a RemoteNode")
    served.add_argument("target", help="module:name of a node, of a function, or (with --make) of a function that makes the node")
    served.add_argument("--listen", default="127.0.0.1:0", metavar="HOST:PORT", help="where to listen (default: this machine, any free port)")
    served.add_argument("--make", action="store_true", help="call the target to make the node")
    served.add_argument("--token-env", default="INTERLACING_TOKEN", metavar="NAME",
                        help="the environment variable holding the token clients must give (default: INTERLACING_TOKEN)")
    return parser


def _node_serve(args, out) -> int:
    """Serve one node until interrupted. The first line written is ``{"address": ...}``."""
    import importlib
    import os
    from .nodes import serve_node
    module, _, name = args.target.partition(":")
    if not name:
        module, _, name = args.target.rpartition(".")
    try:
        found: Any = importlib.import_module(module)
        for part in name.split("."):
            found = getattr(found, part)
    except (ImportError, AttributeError, ValueError) as exc:
        raise SpecError(f"{args.target} cannot be found: {exc}") from None
    node = found() if args.make else found
    host, _, port = args.listen.rpartition(":")
    if not host or not port.isdigit():
        raise SpecError(f"--listen is HOST:PORT, not {args.listen!r}")
    server = serve_node(node, host=host, port=int(port), token=os.environ.get(args.token_env) or None)
    out.write(json.dumps({"address": server.address, "node": server.describe()}) + "\n")
    out.flush()
    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        pass
    finally:
        server.close()
    return 0


def main(argv=None, out=None) -> int:
    import os
    out = out or sys.stdout
    args = _parser().parse_args(argv)
    if os.getcwd() not in sys.path:
        sys.path.insert(0, os.getcwd())       # as with python -m: a module beside where the command is run can be named
    try:
        if args.command == "guide":
            from .spec import Catalog, guide
            out.write(guide(Catalog(allow=args.allow)))
            if not args.allow:
                out.write("Name the modules a graph may take functions from with --allow MODULE, here and on check and run.\n")
            return 0
        if args.command == "check":
            state, code = check(_description(args.graph), allow=args.allow, processes=args.processes, files=_beside(args.graph), tools=args.tools, apps=args.apps,
                                 remotes=args.remote), 0
        elif args.command == "node-serve":
            return _node_serve(args, out)
        elif args.command == "serve":
            from .serve import Server
            Server(args.runs, allow=args.allow, processes=args.processes, workers=args.workers, time_limit=args.time_limit,
                   files=args.files, budget=dict(args.budget), tools=args.tools, apps=args.apps, remotes=args.remote).serve()
            return 0
        else:
            if args.command == "run":
                inputs = _inputs(args)
                state = Run.create(args.dir, _description(args.graph), allow=args.allow, processes=args.processes,
                                   workers=args.workers, files=_beside(args.graph), time_limit=args.time_limit,
                                   workspace=args.workspace, budget=dict(args.budget), tools=args.tools, apps=args.apps,
                                   remotes=args.remote).start(inputs)
            elif args.command == "resume":
                state = Run(args.dir).resume(budget=dict(args.budget))
            elif args.command == "answer":
                state = Run(args.dir).answer(json.loads(args.json) if args.json is not None else args.text, args.question)
            elif args.command == "control":
                state = Run(args.dir).control(args.nodes, args.action, team=args.team)
            elif args.command == "files":
                state = {"files": Run(args.dir).files()}
            else:
                state = Run(args.dir).status()
            code = EXIT.get(state["status"], 1) if args.command in ("run", "resume") else 0
    except (SpecError, JournalError, ValueError) as exc:
        state, code = {"status": "refused", "error": str(exc)}, 1
    encoding = (getattr(out, "encoding", None) or "utf-8").lower().replace("-", "")
    try:
        json.dump(state, out, ensure_ascii=encoding != "utf8", indent=2)     # a console that is not UTF-8 gets escapes
        out.write("\n")
        out.flush()
    except OSError as exc:                    # the reader went away (| head): what it did not read is not an error
        if not isinstance(exc, BrokenPipeError) and not (sys.platform == "win32" and exc.errno == errno.EINVAL):
            raise                             # Windows says EINVAL for a pipe with no reader; anything else is real
        if out is sys.stdout:                 # and Python's own flush at exit must not fail on it either
            os.dup2(os.open(os.devnull, os.O_WRONLY), sys.stdout.fileno())
    return code



if __name__ == "__main__":
    sys.exit(main())
