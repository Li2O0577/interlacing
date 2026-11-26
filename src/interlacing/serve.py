"""The same operations as the command line, offered to a client over the Model Context Protocol.

    interlacing serve --runs DIRECTORY [--allow MODULE]... [--processes] [--workers N] [--time-limit SECONDS]
                        [--files DIRECTORY] [--tools DIRECTORY] [--apps DIRECTORY] [--remote ADDRESS]...
                        [--budget NAME=NUMBER]...

The client (usually a model in some other application) sends graphs as data and gets runs back. What those
graphs may reach is decided by whoever starts the server, on its command line, and never by the client: a
client cannot widen ``--allow``, cannot turn ``--processes`` on, cannot raise ``--budget``, cannot give a node longer than ``--time-limit`` or a block more threads than ``--workers``, cannot choose where a run is kept, and can
take text only from files under ``--files`` (from none when it is not given) tool files only from ``--tools``
applications only from the folders under ``--apps``, and served nodes only at the
``--remote`` addresses.
Model keys come from the server's environment. Every run is a directory under ``--runs``, the same directory
the command line works on, so a run started here can be looked at or answered from a terminal.

The transport is stdio: one JSON-RPC message per line, in on standard input and out on standard output.
Standard output carries nothing else; anything a node prints goes to standard error.

Tools: ``check``, ``start``, ``status``, ``resume``, ``stop``, ``control``, ``answer``, ``runs``. Tool calls are handled
side by side, so a long run does not hold up a question about another. A call to ``start`` or ``resume``
lasts until the run is done, needs a person, or fails; with ``wait: false`` it returns at once and the run
goes on in the server, to be asked about with ``status`` and ended early with ``stop``. A client that cancels
a call (``notifications/cancelled``) stops the run that call was waiting for. When the client goes away, runs
still going are stopped where they can be taken up again.
"""
from __future__ import annotations

import contextlib
import json
import re
import secrets
import sys
import threading
import time
from pathlib import Path
from typing import Any, Callable, Mapping

from . import __version__
from .engine import JournalError
from .runs import Run, check, models_from_environment
from .spec import SpecError

VERSIONS = ("2025-06-18", "2025-03-26", "2024-11-05")
_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}\Z")

_GRAPH = {"type": "object", "description": "The graph as data: nodes, edges, inputs and outputs, or a method with its "
          "nodes. Call guide first for the format and the functions a graph may name."}
_RUN = {"type": "string", "description": "The run's id, as start returned it."}
_WAIT = {"type": "boolean", "description": "false: return at once with status running, and ask with status later. "
         "Default true: return when the run is done, waiting, stopped or failed."}
TOOLS = [
    {"name": "guide", "description": "How a graph is written, with examples, and the functions this server lets a "
     "graph name. Read it before writing a graph.",
     "inputSchema": {"type": "object", "properties": {}}},
    {"name": "check", "description": "Build a graph from its description without running it. Returns its digest, "
     "nodes, inputs and outputs, or says why it cannot be built.",
     "inputSchema": {"type": "object", "properties": {"graph": _GRAPH}, "required": ["graph"]}},
    {"name": "start", "description": "Start a run of a graph. Returns when the run is done, is waiting for a person "
     "(status waiting, with the questions), has stopped, or has failed. Keep the run id it returns.",
     "inputSchema": {"type": "object", "properties": {
         "graph": _GRAPH, "inputs": {"type": "object", "description": "A value for each of the graph's inputs."},
         "wait": _WAIT}, "required": ["graph"]}},
    {"name": "status", "description": "Where a run stands, and the questions in it that are waiting for an answer.",
     "inputSchema": {"type": "object", "properties": {"run": _RUN}, "required": ["run"]}},
    {"name": "show", "description": "Inspect recorded deliveries and dependencies. Include the timeline for pauses "
     "and review verdicts. Use status for final output values.",
     "inputSchema": {"type": "object", "properties": {"run": _RUN, "timeline": {"type": "boolean"}}, "required": ["run"]}},
    {"name": "resume", "description": "Carry on a run that was waiting, stopped or failed. Nothing that had finished runs again.",
     "inputSchema": {"type": "object", "properties": {"run": _RUN, "wait": _WAIT}, "required": ["run"]}},
    {"name": "stop", "description": "Ask a run that is going to stop. Nothing new starts in it; it ends as stopped and "
     "can be resumed.",
     "inputSchema": {"type": "object", "properties": {"run": _RUN}, "required": ["run"]}},
    {"name": "control", "description": "Pause, resume, stop or restart nodes of a run, while it goes or between "
     "goes. pause: it waits at its next step; resume: it goes on; stop: it ends its present work, which is kept for "
     "later; restart: it starts its work again, forgetting what it held.",
     "inputSchema": {"type": "object", "properties": {
         "run": _RUN, "nodes": {"type": "array", "items": {"type": "string"}, "description": "The nodes' ids."},
         "action": {"type": "string", "enum": ["pause", "resume", "stop", "restart"]},
         "team": {"type": "boolean", "description": "Also every node they manage, all the way down."}},
         "required": ["run", "nodes", "action"]}},
    {"name": "answer", "description": "Give a person's answer to a question a run is waiting on. The answer must come "
     "from the person who was asked. Call resume afterwards.",
     "inputSchema": {"type": "object", "properties": {
         "run": _RUN, "answer": {"description": "The answer, any JSON value."},
         "question": {"type": "string", "description": "The question's id; not needed when only one is open."}},
         "required": ["run", "answer"]}},
    {"name": "runs", "description": "The runs this server holds, newest first, with the status of each.",
     "inputSchema": {"type": "object", "properties": {}}},
]


class Refused(Exception):
    """A request the protocol does not allow; becomes a JSON-RPC error."""

    def __init__(self, code: int, message: str):
        super().__init__(message)
        self.code = code


class Server:
    def __init__(self, runs, *, allow=(), processes: bool = False, workers: int = 0,
                 models: Callable[[str], Any] | None = models_from_environment, time_limit: float | None = None, files=None,
                 budget: Mapping[str, float] | None = None, tools=None, apps=None, remotes=()):
        self.root, self.budget, self.tools, self.apps = Path(runs), dict(budget or {}), tools, apps
        self.remotes = list(remotes)
        self.time_limit, self.files = time_limit, files
        self.allow, self.processes, self.workers, self.models = list(allow), bool(processes), int(workers), models
        self._lock = threading.Lock()
        self._going: dict[str, Run] = {}            # the runs this server is running now, by id
        self._asked: dict[Any, str] = {}            # the run each call still in progress is waiting for, by request id
        self._behind: list[threading.Thread] = []   # runs that go on after their call has returned
        self._call = threading.local()              # the request id of the call this thread is answering

    # ---- the operations ----------------------------------------------------------------------------------

    def _run(self, arguments: Mapping) -> Run:
        id = arguments.get("run")
        if not isinstance(id, str) or not _ID.match(id) or not (self.root / id / "graph.json").exists():
            raise SpecError(f"no run {id!r}")
        return Run(self.root / id, models=self.models)

    def _graph(self, arguments: Mapping) -> Mapping:
        graph = arguments.get("graph")
        if not isinstance(graph, Mapping):
            raise SpecError("graph must be an object describing the graph")
        return graph

    def guide(self, arguments: Mapping) -> dict:
        from .spec import GUIDE, Catalog, functions
        return {"guide": GUIDE, "functions": functions(Catalog(allow=self.allow))}

    def check(self, arguments: Mapping) -> dict:
        return check(self._graph(arguments), allow=self.allow, processes=self.processes, files=self.files, tools=self.tools, apps=self.apps,
                     remotes=self.remotes)

    def _carry(self, id: str, run: Run, action: Callable[[], dict], wait: Any) -> dict:
        """Run ``action`` (a start or a resume of ``run``) now, or behind the call when ``wait`` is false."""
        request = getattr(self._call, "id", None)
        with self._lock:
            if id in self._going:
                raise SpecError(f"run {id} is already going")
            self._going[id] = run
            if request is not None:
                self._asked[request] = id

        def go() -> dict:
            try:
                return action()
            finally:
                with self._lock:
                    self._going.pop(id, None)
                    if self._asked.get(request) == id:
                        del self._asked[request]

        if wait is not False:
            return {"run": id, **go()}

        def behind() -> None:
            try:
                go()
            except Exception as exc:                                # noqa: BLE001 - nobody is waiting to be told
                run._state("failed", error=f"{type(exc).__name__}: {exc}")
        thread = threading.Thread(target=behind, name=f"run-{id}", daemon=True)
        with self._lock:
            self._behind = [t for t in self._behind if t.is_alive()] + [thread]
        thread.start()
        return {"run": id, "status": "running", "questions": []}

    def start(self, arguments: Mapping) -> dict:
        inputs = arguments.get("inputs") or {}
        if not isinstance(inputs, Mapping):
            raise SpecError("inputs must be an object: a value for each graph input")
        id = f"{time.strftime('%Y%m%d-%H%M%S', time.gmtime())}-{secrets.token_hex(3)}"
        run = Run.create(self.root / id, self._graph(arguments), allow=self.allow, processes=self.processes,
                         workers=self.workers, models=self.models, time_limit=self.time_limit, files=self.files,
                         budget=self.budget, strict=True, tools=self.tools, apps=self.apps,
                         remotes=self.remotes)
        return self._carry(id, run, lambda: run.start(inputs), arguments.get("wait"))

    def status(self, arguments: Mapping) -> dict:
        state = self._run(arguments).status()
        if arguments["run"] in self._going:         # going, though the engine may not have taken the lock yet
            state = {"status": "running", "graph": state.get("graph"), "updated": state.get("updated"),
                     "questions": state["questions"]}
        return {"run": arguments["run"], **state}

    def show(self, arguments: Mapping) -> dict:
        timeline = arguments.get("timeline", False)
        if not isinstance(timeline, bool):
            raise SpecError("timeline must be a boolean")
        return {"run": arguments["run"], **self._run(arguments).show(timeline=timeline)}

    def resume(self, arguments: Mapping) -> dict:
        run = self._run(arguments)
        return self._carry(arguments["run"], run, run.resume, arguments.get("wait"))

    def stop(self, arguments: Mapping) -> dict:
        self._run(arguments)
        run = self._going.get(arguments["run"])
        if run is None:
            raise SpecError(f"run {arguments['run']} is not going in this server")
        while not run.stop() and arguments["run"] in self._going:
            time.sleep(0.005)                       # it has been taken on and its engine is about to exist
        return {"run": arguments["run"], "status": "stopping"}

    def control(self, arguments: Mapping) -> dict:
        run = self._run(arguments)
        nodes, action = arguments.get("nodes"), arguments.get("action")
        if not isinstance(nodes, list) or not all(isinstance(n, str) for n in nodes) or not nodes:
            raise SpecError("nodes must be a list of node ids")
        going = self._going.get(arguments["run"])
        if going is not None:
            while going._now is None and arguments["run"] in self._going:
                time.sleep(0.005)                   # taken on: its engine is about to exist
            if going._now is not None:
                return {"run": arguments["run"], **going.control(nodes, str(action), team=bool(arguments.get("team")))}
        return {"run": arguments["run"], **run.control(nodes, str(action), team=bool(arguments.get("team")))}

    def answer(self, arguments: Mapping) -> dict:
        if "answer" not in arguments:
            raise SpecError("answer is missing")
        return {"run": arguments["run"], **self._run(arguments).answer(arguments["answer"], arguments.get("question"))}

    def runs(self, arguments: Mapping) -> dict:
        found = []
        for path in sorted(self.root.glob("*/state.json"), key=lambda p: p.parent.name, reverse=True):
            state = json.loads(path.read_text(encoding="utf-8"))
            going = path.parent.name in self._going
            found.append({"run": path.parent.name, "status": "running" if going else state["status"], "updated": state["updated"]})
        return {"runs": found}

    # ---- the protocol ------------------------------------------------------------------------------------

    def handle(self, message: Any) -> dict | None:
        """Answer one JSON-RPC message. Notifications get no answer."""
        if not isinstance(message, Mapping) or not isinstance(message.get("method"), str):
            if isinstance(message, Mapping) and ("result" in message or "error" in message):
                return None                                        # a reply to something we never asked
            return _error(None, -32600, "not a JSON-RPC request")
        id, method, params = message.get("id"), message["method"], message.get("params") or {}
        if "id" not in message:
            if method == "notifications/cancelled" and isinstance(params, Mapping):
                run = self._going.get(self._asked.get(params.get("requestId"), ""))
                if run is not None:                 # the client no longer waits for it: it stops, to be resumed
                    run.stop()
            return None
        self._call.id = id
        try:
            return {"jsonrpc": "2.0", "id": id, "result": self._method(method, params)}
        except Refused as exc:
            return _error(id, exc.code, str(exc))
        finally:
            self._call.id = None

    def _method(self, method: str, params: Mapping) -> dict:
        if method == "initialize":
            asked = params.get("protocolVersion")
            return {"protocolVersion": asked if asked in VERSIONS else VERSIONS[0],
                    "capabilities": {"tools": {}}, "serverInfo": {"name": "interlacing", "version": __version__},
                    "instructions": "Runs graphs of nodes written as data. Read guide for the format, check a graph, start it, and when a run "
                                    "is waiting put its questions to the person, answer, then resume."}
        if method == "ping":
            return {}
        if method == "tools/list":
            return {"tools": TOOLS}
        if method == "tools/call":
            name, arguments = params.get("name"), params.get("arguments") or {}
            if name not in {tool["name"] for tool in TOOLS}:
                raise Refused(-32602, f"no tool {name!r}")
            if not isinstance(arguments, Mapping):
                raise Refused(-32602, "arguments must be an object")
            try:
                state, failed = getattr(self, name)(arguments), False
            except (SpecError, JournalError) as exc:
                state, failed = {"status": "refused", "error": str(exc)}, True
            except Exception as exc:                                # the server outlives a call that goes wrong
                state, failed = {"status": "error", "error": f"{type(exc).__name__}: {exc}"}, True
            return {"content": [{"type": "text", "text": json.dumps(state, ensure_ascii=False, indent=2, default=str)}],
                    "structuredContent": json.loads(json.dumps(state, default=str)), "isError": failed}
        raise Refused(-32601, f"no method {method!r}")

    def close(self) -> None:
        """Stop the runs that are going behind their calls and wait for them to have stopped."""
        with self._lock:
            runs, threads = list(self._going.values()), list(self._behind)
        for thread in threads:
            while thread.is_alive():
                for run in runs:
                    run.stop()
                thread.join(0.02)

    def serve(self, lines=None, out=None) -> None:
        """Read messages, one per line, until the input ends. Tool calls are answered side by side, each as it
        is done; everything else is answered in turn."""
        if lines is None and out is None:
            for stream in (sys.stdin, sys.stdout):          # the protocol is UTF-8 whatever the console's encoding is
                if hasattr(stream, "reconfigure"):
                    stream.reconfigure(encoding="utf-8", newline="\n")
        lines, out = lines if lines is not None else sys.stdin, out or sys.stdout
        writing = threading.Lock()

        def send(reply: dict | None) -> None:
            if reply is not None:
                with writing:
                    out.write(json.dumps(reply, ensure_ascii=False) + "\n")
                    out.flush()

        calls: list[threading.Thread] = []
        with contextlib.redirect_stdout(sys.stderr):                # what a node prints must not reach the client
            for line in lines:
                if not line.strip():
                    continue
                try:
                    message = json.loads(line)
                except ValueError:
                    send(_error(None, -32700, "not JSON"))
                    continue
                if isinstance(message, Mapping) and message.get("method") == "tools/call" and "id" in message:
                    call = threading.Thread(target=lambda message=message: send(self.handle(message)), daemon=True)
                    calls = [thread for thread in calls if thread.is_alive()] + [call]
                    call.start()
                else:
                    send(self.handle(message))
            for call in calls:                                      # the client has gone: finish what it asked for,
                call.join()
            self.close()                                            # and leave nothing running behind


def _error(id, code: int, message: str) -> dict:
    return {"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message}}

