"""A run kept in a directory: the graph, its journal, the questions put to people and their answers.

Everything about a run is in its directory, so a run can be started by one process, answered from another
and continued by a third::

    graph.json      the graph, as data
    options.json    what the graph may reach (modules, processes), how many workers, the time limit, the budget
    inputs.json     what the run was started with
    journal.jsonl   what has happened so far
    questions/      one file per question that a person in the graph has been asked
    answers/        one file per answer given
    files/          the workspace of the graph's nodes: what the ``file`` protocol's paths are relative to, with
                    a directory for each node that works there (``Run.files()`` says who wrote what)
    controls/       switches set from another process while this run is going (see ``Run.control``),
                    waiting to be taken up by the process that runs it
    state.json      where the run stands: done, waiting, stopped or failed (``running`` is told from the lock)
    lock            there while a process is running the graph, and names that process

One process at a time runs a run. A second that tries is refused and told who holds it; a lock left by a
process that has died is taken over.

A person is never waited for inside a running process. When a ``human`` node needs an answer that is not
there yet, the run stops with the state ``waiting``; once the answer has been given, ``resume`` carries on
from the journal. Nothing that had finished runs again.
"""
from __future__ import annotations

import json
import os
import socket
import sys
import threading
import time
from pathlib import Path
from typing import Any, Callable, Mapping

from .engine import Engine, Journal, RunError
from .nodes import Workspace
from .models import ChatResult, FakeProvider, ProviderError, build
from .spec import Catalog, SpecError, dump, load, load_node

DONE, WAITING, STOPPED, FAILED, NEW, RUNNING = "done", "waiting", "stopped", "failed", "new", "running"


class Pending(BaseException):
    """Raised inside a ``human`` node whose answer is not there yet. It is not an error: the engine lets it
    pass, the journal stays open, and the run is continued later."""

    def __init__(self, question: Mapping):
        super().__init__(question["id"])
        self.question = dict(question)


def models_from_environment(name: str) -> Any:
    """The model ``provider:model`` stands for, with keys taken from the environment and from nowhere else.

    ``anthropic:<model>`` uses ``ANTHROPIC_API_KEY`` (and ``ANTHROPIC_BASE_URL`` when set);
    ``openai:<model>`` uses ``OPENAI_API_KEY`` and ``OPENAI_BASE_URL`` (default ``https://api.openai.com/v1``);
    ``fake:echo`` calls no service and repeats what it was told, for trying a graph out.
    """
    provider, _, model = name.partition(":")
    if provider == "fake":
        echo = FakeProvider(lambda system, messages, tools: ChatResult(text=str(messages[-1].get("content", ""))))
        echo.name = name
        return echo
    env = os.environ
    known = [{"id": "anthropic", "type": "anthropic", "api_key": env.get("ANTHROPIC_API_KEY", ""),
              "base_url": env.get("ANTHROPIC_BASE_URL", "")},
             {"id": "openai", "type": "openai_compat", "api_key": env.get("OPENAI_API_KEY", ""),
              "base_url": env.get("OPENAI_BASE_URL", "https://api.openai.com/v1")}]
    key = {"anthropic": "ANTHROPIC_API_KEY", "openai": "OPENAI_API_KEY"}.get(provider)
    if key and not env.get(key):
        raise SpecError(f"the model {name!r} needs {key} in the environment")
    try:
        return build(name, known)
    except ProviderError as exc:
        raise SpecError(f"the model {name!r} cannot be built: {exc} (known providers: anthropic, openai, fake)") from None


class _Unbuilt:
    """Stands in for a model while a graph is only being checked: no key is needed and nothing is called."""

    def __init__(self, name: str):
        self.name = name

    def chat(self, *args, **kwargs):
        raise RuntimeError("a graph that is only being checked does not run")


def check(description: Mapping, *, allow=(), processes: bool = False, files=None, tools=None, apps=None,
          remotes=()) -> dict:
    """Build the graph a description stands for, without running anything, and say what it is."""
    graph = load(description, Catalog(allow=allow, processes=processes, models=_Unbuilt, files=files, workspace=".", tools=tools, apps=apps,
                                     remotes=remotes))
    return {"digest": graph.digest, "nodes": {node.id: type(node).kind for node in graph.nodes},
            "edges": len(graph.edges), "inputs": sorted(graph.inputs), "outputs": sorted(graph.outputs)}


def _budget(budget: Mapping[str, float] | None) -> dict:
    budget = dict(budget or {})
    for name, limit in budget.items():
        if isinstance(limit, bool) or not isinstance(limit, (int, float)) or not limit >= 0:
            raise SpecError(f"budget: {name} must be a number that is not negative, not {limit!r}")
    return budget


class _Lock:
    """Held by the one process that is running the graph of a run directory."""

    def __init__(self, directory: Path):
        self.directory, self.path = directory, directory / "lock"

    def __enter__(self):
        me = {"pid": os.getpid(), "host": socket.gethostname(), "since": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
        for _ in range(3):
            try:
                handle = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            except FileExistsError:
                try:
                    holder = json.loads(self.path.read_text(encoding="utf-8"))
                except (OSError, ValueError):
                    time.sleep(0.05)                       # being written or just released: look again
                    continue
                if holder.get("host") == me["host"] and holder.get("pid") != me["pid"] and not _alive(holder.get("pid")):
                    stale = self.path.with_name(f"lock.stale-{me['pid']}-{time.monotonic_ns()}")
                    try:
                        os.rename(self.path, stale)        # only one of several taking it over gets this far
                        os.unlink(stale)
                    except OSError:
                        pass
                    continue
                raise SpecError(f"{self.directory} is being run by process {holder.get('pid')} on {holder.get('host')} "
                                f"since {holder.get('since')}; if that process is gone, delete {self.path}")
            with os.fdopen(handle, "w", encoding="utf-8") as file:
                json.dump(me, file)
            return self
        raise SpecError(f"{self.directory} is being run by another process")

    def __exit__(self, *exc):
        try:
            os.unlink(self.path)
        except OSError:
            pass
        return False


def _holder(directory: Path) -> dict | None:
    """Who is running the graph of a run directory now, or None."""
    try:
        holder = json.loads((directory / "lock").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if holder.get("host") == socket.gethostname() and holder.get("pid") != os.getpid() and not _alive(holder.get("pid")):
        return None                                        # left behind by a process that has died
    return holder


def _alive(pid) -> bool:
    """Whether a process of this machine is still there. Where that cannot be asked safely, it is taken to be."""
    if type(pid) is not int or pid <= 0 or pid > 0xffffffff:
        return True
    if sys.platform == "win32":
        # A zero-time wait only observes termination. Windows os.kill does not offer the POSIX probe.
        import _winapi
        try:
            handle = _winapi.OpenProcess(_winapi.SYNCHRONIZE, False, pid)
        except OSError as exc:
            return exc.winerror != 87  # ERROR_INVALID_PARAMETER: no process has that PID
        try:
            return _winapi.WaitForSingleObject(handle, 0) != _winapi.WAIT_OBJECT_0
        finally:
            _winapi.CloseHandle(handle)
    if os.name != "posix":
        return True
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except OSError:
        return True
    return True


def _write(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.part")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def _read(path: Path, default: Any = None) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return default


class Run:
    """``observe`` is told what happens while the run is going, as the engine's ``observe`` is."""

    def __init__(self, directory, *, models: Callable[[str], Any] | None = models_from_environment,
                 observe: Callable[[Mapping], None] | None = None):
        self.directory = Path(directory)
        self.models, self.observe = models, observe
        self._now: Engine | None = None     # the engine, while this object is running the graph

    # ---- creating --------------------------------------------------------------------------------------

    @classmethod
    def create(cls, directory, description: Mapping, *, allow=(), processes: bool = False, workers: int = 0,
               models: Callable[[str], Any] | None = models_from_environment, files=None,
               time_limit: float | None = None, observe: Callable[[Mapping], None] | None = None,
               workspace=None, budget: Mapping[str, float] | None = None, strict: bool = False, tools=None,
               apps=None, remotes=()) -> "Run":
        """Check the description and set up a directory for one run of it. Nothing runs yet. ``files`` is
        the directory the description may take text from; the text is kept in the run, the files are not
        needed again. ``time_limit`` is the longest one activation may take, in seconds. ``workspace`` is the
        directory the nodes work in and send file paths relative to; it is ``files`` in the run's directory
        unless another is given. ``budget`` is the most the run may use of what its nodes report using,
        by name: ``{"tokens": 200000}``. ``strict=True`` is for a description that is not trusted: no node
        in it may give itself more time than ``time_limit``, and no block more threads than ``workers``.
        ``tools`` is a tool pool: the directory the graph's agents may take tool files from. ``apps`` is the
        directory whose folders may be nodes, and ``remotes`` the addresses
        of served nodes the graph may reach."""
        run = cls(directory, models=models, observe=observe)
        if (run.directory / "graph.json").exists():
            raise SpecError(f"{run.directory} already holds a run")
        if time_limit is not None and not time_limit > 0:
            raise SpecError("the time limit must be a positive number of seconds")
        options = {"allow": list(allow), "processes": bool(processes), "workers": int(workers), "time_limit": time_limit,
                   "budget": _budget(budget), "strict": bool(strict),
                   "workspace": str(Path(workspace).resolve()) if workspace is not None else None,
                   "tools": str(Path(tools).resolve()) if tools is not None else None,
                   "apps": str(Path(apps).resolve()) if apps is not None else None, "remotes": list(remotes)}
        run.workspace(options).mkdir(parents=True, exist_ok=True)
        graph = load(description, run._catalog(options, files))
        _write(run.directory / "graph.json", dump(graph))
        _write(run.directory / "options.json", options)
        run._state(NEW, graph=graph.digest)
        return run

    def workspace(self, options: Mapping | None = None) -> Path:
        """The directory the graph's nodes work in."""
        options = options if options is not None else _read(self.directory / "options.json", {})
        return Path(options["workspace"]) if options.get("workspace") else self.directory / "files"

    def files(self) -> list[dict]:
        """Every file in the run's workspace, with the node that wrote it and in which activation, where
        it was written through a node's area."""
        self._held()
        return Workspace(self.workspace()).files()

    def _catalog(self, options: Mapping, files=None) -> Catalog:
        return Catalog(allow=options["allow"], processes=options["processes"], models=self.models, ask=self._ask, files=files,
                       workspace=self.workspace(options), max_workers=options["workers"] if options.get("strict") else None,
                       tools=options.get("tools"), apps=options.get("apps"),
                       remotes=options.get("remotes", ()))

    def _engine(self) -> Engine:
        description = _read(self.directory / "graph.json")
        if description is None:
            raise SpecError(f"{self.directory} holds no run")
        options = _read(self.directory / "options.json")
        catalog = self._catalog(options)
        return Engine(load(description, catalog), catalog.protocols, workers=options["workers"],
                      journal=Journal(self.directory / "journal.jsonl"), time_limit=options.get("time_limit"),
                      observe=self.observe, budget=options.get("budget"), firm=bool(options.get("strict")),
                      rebuild=lambda described: load_node(described, catalog))

    # ---- running ---------------------------------------------------------------------------------------

    def start(self, inputs: Mapping[str, Any] | None = None) -> dict:
        """Run from the beginning until the run is done, needs a person, or fails. Returns the state."""
        self._held()
        if (self.directory / "journal.jsonl").exists():
            raise SpecError(f"{self.directory} has already been started: resume it")
        try:
            _write(self.directory / "inputs.json", dict(inputs or {}))
        except (TypeError, ValueError) as exc:
            raise SpecError(f"the inputs are not plain data: {exc}") from None
        return self._go(lambda engine: engine.run(inputs))

    def resume(self, *, budget: Mapping[str, float] | None = None) -> dict:
        """Carry on from the journal. ``budget`` replaces the limits it names: a run that failed for going
        over its budget goes on under a larger one."""
        self._held()
        if not (self.directory / "journal.jsonl").exists():
            raise SpecError(f"{self.directory} has not been started")
        journal = Journal(self.directory / "journal.jsonl")
        if not journal.read():                      # the process died before the first record was whole
            return self._go(lambda engine: engine.run(_read(self.directory / "inputs.json", {})), budget)
        return self._go(lambda engine: engine.resume(), budget)

    def _held(self) -> None:
        if not (self.directory / "graph.json").exists():
            raise SpecError(f"{self.directory} holds no run")

    def _go(self, action, budget: Mapping[str, float] | None = None) -> dict:
        with _Lock(self.directory):
            if budget:
                options = _read(self.directory / "options.json")
                _write(self.directory / "options.json", {**options, "budget": {**(options.get("budget") or {}), **_budget(budget)}})
            self._absorb()                          # switches set while no process ran it
            engine = self._now = self._engine()
            over = threading.Event()
            watcher = threading.Thread(target=self._watch, args=(engine, over), name="run-controls", daemon=True)
            watcher.start()
            try:
                result = action(engine)
            except Pending:
                return self._state(WAITING, used=engine.used)
            except RunError as exc:
                return self._state(FAILED, error=str(exc), node=exc.node, problems=exc.problems, used=exc.used)
            finally:
                self._now = None
                over.set()
                watcher.join()
                self._absorb()                      # what came too late for this run holds for the next
            return self._state(DONE if result.complete else STOPPED, outputs=result.outputs, rounds=result.rounds,
                               limited=list(result.limited), problems=result.problems, used=result.used,
                               **({"held": result.held} if result.held else {}))

    def stop(self) -> bool:
        """Ask the run this object is running to stop: nothing new starts, and it ends as ``stopped``, to be
        resumed later. False when this object is not running it."""
        engine = self._now
        if engine is None:
            return False
        engine.stop()
        return True

    def control(self, nodes: Any, action: str, *, team: bool = False) -> dict:
        """Pause, resume, stop or restart nodes of this run (see ``Engine.control``). While this object runs
        it, at once. While another process runs it, the change is left in ``controls/`` and that process
        takes it up within a moment. Otherwise it is written to the journal and holds when the run is taken
        up. ``changed`` names the nodes the change was for; ``switches`` is where they stand."""
        engine = self._now
        if engine is not None:
            changed = engine.control(nodes, action, team=team)
            return {"status": RUNNING, "changed": changed, "switches": self.switches()}
        self._held()
        from .engine.recorder import SWITCHED
        if action not in SWITCHED:
            raise SpecError(f"no action {action!r}: one of {', '.join(SWITCHED)}")
        graph = self._engine().graph
        path = self.directory / "journal.jsonl"
        known = {node.id for node in graph.nodes}
        if path.exists():
            known |= {r["node"] for r in Journal(path).read() if r["kind"] == "act"}
        ids = [nodes] if isinstance(nodes, str) else list(nodes)
        missing = [id for id in ids if id not in known]
        if missing:
            raise SpecError(f"no node {', '.join(missing)}")
        if team:
            ids = [*ids, *(member for id in ids for member in graph.team(id))]
        ids = list(dict.fromkeys(ids))
        request = {"nodes": ids, "action": action}
        if _holder(self.directory) is None:
            if not path.exists():
                raise SpecError(f"{self.directory} has not been started")
            try:
                with _Lock(self.directory):
                    before = self.switches()
                    changed = [id for id in ids if action == "restart" or before.get(id, "active") != SWITCHED[action]]
                    if changed:
                        journal = Journal(path)
                        journal.write({"kind": "control", "nodes": changed, "action": action})
                        journal.close()
                return {**self.status(), "changed": changed}
            except SpecError:
                pass                                # a process took it up meanwhile: leave it for that one
        _write(self.directory / "controls" / f"{time.time_ns()}-{os.getpid()}.json", request)
        return {**self.status(), "changed": ids, "sent": True}

    def _controls(self) -> list[Path]:
        return sorted((self.directory / "controls").glob("*.json"))

    def _watch(self, engine: Engine, over: threading.Event) -> None:
        """While this process runs the graph, take up switches set from other processes."""
        while not over.wait(0.1):
            for path in self._controls():
                request = _read(path)
                if not engine.going:
                    break                           # not started yet, or over: taken up later
                try:
                    if request:
                        engine.control(request["nodes"], request["action"])
                except RunError:
                    if not engine.going:
                        break                       # it ended meanwhile: taken up later
                except (ValueError, KeyError, TypeError):
                    pass                            # not a request this run can act on
                path.unlink(missing_ok=True)

    def _absorb(self) -> None:
        """Switches left in ``controls/`` when no process is taking them up go into the journal. Holds the lock."""
        waiting = self._controls()
        path = self.directory / "journal.jsonl"
        if not waiting or not path.exists():
            return
        journal = Journal(path)
        for each in waiting:
            request = _read(each)
            if request:
                journal.write({"kind": "control", "nodes": list(request["nodes"]), "action": request["action"]})
            each.unlink(missing_ok=True)
        journal.close()

    def switches(self) -> dict[str, str]:
        """The nodes whose switch is not on, as the journal has it: node -> paused or stopped."""
        from .engine.recorder import SWITCHED
        path = self.directory / "journal.jsonl"
        found: dict[str, str] = {}
        records = Journal(path).read() if path.exists() else []
        sent = [request for request in map(_read, self._controls()) if request]      # not yet taken up
        for record in [*records, *({"kind": "control", **request} for request in sent)]:
            if record["kind"] == "control" and record.get("action") in SWITCHED:
                for id in record["nodes"]:
                    found[id] = SWITCHED[record["action"]]
        return {id: status for id, status in found.items() if status != "active"}

    # ---- people ----------------------------------------------------------------------------------------

    def _ask(self, node: str, round: int, request: Any) -> Any:
        id = f"{node}-{round}".replace("/", "__")       # a person inside a block: block.round__node-round
        answer = _read(self.directory / "answers" / f"{id}.json")
        if answer is not None:
            return answer["answer"]
        question = {"id": id, "node": node, "round": round, "request": request}
        _write(self.directory / "questions" / f"{id}.json", question)
        raise Pending(question)

    def questions(self) -> list[dict]:
        """The questions that have been asked and not yet answered, oldest first."""
        asked = sorted((self.directory / "questions").glob("*.json"), key=lambda path: path.stat().st_mtime)
        return [_read(path) for path in asked if not (self.directory / "answers" / path.name).exists()]

    def answer(self, answer: Any, question: str | None = None) -> dict:
        """Give the answer to a question (to the only open one when none is named). The run is not resumed."""
        waiting = self.questions()
        if question is None:
            if len(waiting) != 1:
                raise SpecError(f"{len(waiting)} questions are open: name the one this answers"
                                if waiting else "no question is open")
            question = waiting[0]["id"]
        elif question not in {q["id"] for q in waiting}:
            raise SpecError(f"no open question {question!r}")
        _write(self.directory / "answers" / f"{question}.json", {"answer": answer})
        return {**self.status(), "answered": question, "next": "resume the run to carry on with the answer"}

    # ---- state -----------------------------------------------------------------------------------------

    def _state(self, status: str, **more) -> dict:
        previous = _read(self.directory / "state.json", {})
        state = {"status": status, "graph": more.pop("graph", previous.get("graph")),
                 "updated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), **more}
        _write(self.directory / "state.json", state)
        return {**state, "questions": self.questions()}

    def show(self, *, timeline: bool = False) -> dict:
        """Inspect the saved graph and journal without constructing or running nodes."""
        from .inspection import explain_records
        self._held()
        state = _read(self.directory / "state.json", {})
        status = RUNNING if _holder(self.directory) else state.get("status", NEW)
        records = Journal(self.directory / "journal.jsonl").inspect()
        view = explain_records(_read(self.directory / "graph.json"), records,
                               status, self.questions(), timeline=timeline)
        if status == FAILED:
            view["failure"] = {k: state[k] for k in ("node", "error") if k in state}
        return view

    def status(self) -> dict:
        state = _read(self.directory / "state.json")
        if state is None:
            raise SpecError(f"{self.directory} holds no run")
        if _holder(self.directory) is not None:      # being run now: what state.json holds is from before
            state = {"status": RUNNING, "graph": state.get("graph"), "updated": state.get("updated")}
        switches = self.switches()
        return {**state, **({"switches": switches} if switches else {}), "questions": self.questions()}
