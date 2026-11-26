"""A run kept in a directory: the graph, its journal, the questions put to people and their answers.

Everything about a run is in its directory, so a run can be started by one process, answered from another
and continued by a third::

    graph.json      the graph, as data
    options.json    what the graph may reach (modules, processes), how many workers, the time limit, the budget
    inputs.json     what the run was started with
    journal.jsonl or journal.sqlite3  what has happened so far
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

import hashlib
import json
import os
import secrets
import shutil
import socket
import sys
import threading
import time
from itertools import chain
from pathlib import Path
from typing import Any, Callable, Mapping

from .base import GraphError, canonical
from .engine import Engine, Journal, RunError, SQLiteJournal
from .engine.durable import DurableState
from .engine.journal import pack_body
from .engine.wiring import INPUT
from .forking import output_correction
from .nodes import Workspace
from .models import ChatResult, FakeProvider, ProviderError, build
from .spec import Catalog, SpecError, dump, load, load_node

DONE, WAITING, STOPPED, FAILED, NEW, RUNNING = "done", "waiting", "stopped", "failed", "new", "running"
JOURNALS = ("jsonl", "sqlite")


def fork_inputs(graph: dict, state: DurableState, inputs: Mapping[str, Any]) -> list:
    from .spec.kinds import schema_of
    nodes = {node["id"]: node for node in graph["nodes"]}
    overrides = []
    for name, value in inputs.items():
        if name not in graph["inputs"]:
            raise SpecError(f"the graph has no input {name!r}")
        edge = INPUT + name
        sent = state.data["counters"].get(edge, 0)
        previous = f"{edge}#{sent}"
        if previous not in state.data["mail"] and state.seq != 1:
            raise SpecError(f"graph input {name!r} is no longer available to override at this position")
        node, _, port = graph["inputs"][name].rpartition(".")
        described = next(item for item in nodes[node]["inputs"] if item["name"] == port)
        try:
            schema_of(described.get("schema")).check(value)
            json.dumps(value, allow_nan=False)
        except (TypeError, ValueError) as exc:
            raise SpecError(f"graph input {name}: {exc}") from None
        overrides.append([edge, sent + 1, 0, pack_body(value), [], *([previous] if sent else [])])
    return overrides


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
               apps=None, remotes=(), journal: str = "jsonl", durable: bool = False) -> "Run":
        """Check the description and set up a directory for one run of it. Nothing runs yet. ``files`` is
        the directory the description may take text from; the text is kept in the run, the files are not
        needed again. ``time_limit`` is the longest one activation may take, in seconds. ``workspace`` is the
        directory the nodes work in and send file paths relative to; it is ``files`` in the run's directory
        unless another is given. ``budget`` is the most the run may use of what its nodes report using,
        by name: ``{"tokens": 200000}``. ``strict=True`` is for a description that is not trusted: no node
        in it may give itself more time than ``time_limit``, and no block more threads than ``workers``.
        ``tools`` is a tool pool: the directory the graph's agents may take tool files from. ``apps`` is the
        directory whose folders may be nodes, and ``remotes`` the addresses
        of served nodes the graph may reach. ``journal`` selects JSONL or SQLite. ``durable`` enables
        SQLite delivery commits and recovery checkpoints."""
        run = cls(directory, models=models, observe=observe)
        if time_limit is not None and not time_limit > 0:
            raise SpecError("the time limit must be a positive number of seconds")
        if journal not in JOURNALS:
            raise SpecError(f"unknown journal backend {journal!r}: choose jsonl or sqlite")
        if durable and journal != "sqlite":
            raise SpecError("durable delivery requires the SQLite journal")
        options = {"allow": list(allow), "processes": bool(processes), "workers": int(workers), "time_limit": time_limit,
                   "budget": _budget(budget), "strict": bool(strict),
                   "workspace": str(Path(workspace).resolve()) if workspace is not None else None,
                   "tools": str(Path(tools).resolve()) if tools is not None else None,
                   "apps": str(Path(apps).resolve()) if apps is not None else None, "remotes": list(remotes)}
        if journal != "jsonl":
            options["journal"] = journal
        if durable:
            options["durable"] = True
        run.directory.mkdir(parents=True, exist_ok=True)
        with _Lock(run.directory):
            if (run.directory / "graph.json").exists():
                raise SpecError(f"{run.directory} already holds a run")
            run.workspace(options).mkdir(parents=True, exist_ok=True)
            graph = load(description, run._catalog(options, files))
            if durable:
                try:
                    Engine._check_durable_graph(graph)
                except GraphError as exc:
                    raise SpecError(str(exc)) from exc
            saved_graph = dump(graph)
            _write(run.directory / "options.json", options)
            run._state(NEW, graph=graph.digest)
            _write(run.directory / "graph.json", saved_graph)
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

    def _journal(self, options: Mapping | None = None) -> Journal | SQLiteJournal:
        options = options if options is not None else _read(self.directory / "options.json", {})
        backend = options.get("journal", "jsonl")
        if backend == "jsonl":
            return Journal(self.directory / "journal.jsonl")
        if backend == "sqlite":
            return SQLiteJournal(self.directory / "journal.sqlite3", durable=bool(options.get("durable")))
        raise SpecError(f"unknown journal backend {backend!r} in {self.directory}")

    def _engine(self) -> Engine:
        description = _read(self.directory / "graph.json")
        if description is None:
            raise SpecError(f"{self.directory} holds no run")
        options = _read(self.directory / "options.json")
        catalog = self._catalog(options)
        return Engine(load(description, catalog), catalog.protocols, workers=options["workers"],
                      journal=self._journal(options), time_limit=options.get("time_limit"),
                      observe=self.observe, budget=options.get("budget"), firm=bool(options.get("strict")),
                      rebuild=lambda described: load_node(described, catalog))

    # ---- running ---------------------------------------------------------------------------------------

    def start(self, inputs: Mapping[str, Any] | None = None) -> dict:
        """Run from the beginning until the run is done, needs a person, or fails. Returns the state."""
        self._held()
        with _Lock(self.directory):
            self._require_execution()
            if self._journal().path.exists():
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
        with _Lock(self.directory):
            self._require_execution()
            journal = self._journal()
            if not journal.path.exists():
                raise SpecError(f"{self.directory} has not been started")
            has_records = journal.has_records() if isinstance(journal, SQLiteJournal) else bool(journal.read())
            if not has_records:                     # the process died before the first record was whole
                return self._go(lambda engine: engine.run(_read(self.directory / "inputs.json", {})), budget)
            return self._go(lambda engine: engine.resume(), budget)

    def _held(self) -> None:
        if not (self.directory / "graph.json").exists():
            raise SpecError(f"{self.directory} holds no run")

    def _require_execution(self) -> None:
        fork = _read(self.directory / "options.json", {}).get("fork")
        if fork and not fork["executable"]:
            raise SpecError("this fork is for inspection; create an executable fork to run or control it")

    def _go(self, action, budget: Mapping[str, float] | None = None) -> dict:
        """Run the engine and its control watcher while the caller holds the directory lock."""
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
        self._require_execution()
        from .engine.recorder import Recorder, SWITCHED
        if action not in SWITCHED:
            raise SpecError(f"no action {action!r}: one of {', '.join(SWITCHED)}")
        graph = self._engine().graph
        journal = self._journal()
        if isinstance(journal, SQLiteJournal) and journal.durable and action == "restart":
            raise SpecError("durable activations support pause, stop and resume")
        known = {node.id for node in graph.nodes}
        known |= {r["node"] for r in journal.inspect() if r["kind"] == "act"}
        ids = [nodes] if isinstance(nodes, str) else list(nodes)
        missing = [id for id in ids if id not in known]
        if missing:
            raise SpecError(f"no node {', '.join(missing)}")
        if team:
            ids = [*ids, *(member for id in ids for member in graph.team(id))]
        ids = list(dict.fromkeys(ids))
        request = {"nodes": ids, "action": action}
        if _holder(self.directory) is None:
            if not journal.path.exists():
                raise SpecError(f"{self.directory} has not been started")
            try:
                with _Lock(self.directory):
                    before = self.switches()
                    changed = [id for id in ids if action == "restart" or before.get(id, "active") != SWITCHED[action]]
                    if changed:
                        recorder = Recorder(journal)
                        recorder.control(changed, action)
                        recorder.close()
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
        from .engine.recorder import Recorder
        waiting = self._controls()
        if not waiting:
            return
        journal = self._journal()
        if not journal.path.exists():
            return
        recorder = Recorder(journal)
        for each in waiting:
            request = _read(each)
            if request:
                recorder.control(list(request["nodes"]), request["action"])
            each.unlink(missing_ok=True)
        recorder.close()

    def switches(self) -> dict[str, str]:
        """The nodes whose switch is not on, as the journal has it: node -> paused or stopped."""
        from .engine.recorder import SWITCHED
        found: dict[str, str] = {}
        records = self._journal().inspect()
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

    def _history_journal(self) -> SQLiteJournal:
        self._held()
        journal = self._journal()
        if not isinstance(journal, SQLiteJournal) or not journal.durable:
            raise SpecError("historical positions require a durable SQLite run")
        return journal

    def checkpoints(self) -> dict:
        """List retained recovery positions and the last committed root event."""
        last, positions = self._history_journal().positions()
        return {"last_event": last, "checkpoints": positions}

    @staticmethod
    def _history_at(journal: SQLiteJournal, at: int) -> tuple[int | None, DurableState]:
        if type(at) is not int:
            raise SpecError("an event position must be an integer")
        if not journal.path.exists():
            raise SpecError("the run has no committed events")
        return DurableState.load(journal, at=at)

    def fork(self, directory, *, at: int, inputs: Mapping[str, Any] | None = None,
             execute: bool = False, output: dict | None = None) -> Run:
        """Copy a settled root position into a new run. Only an executable fork may be resumed."""
        journal = self._history_journal()
        _, state = self._history_at(journal, at)
        if state.pending:
            raise SpecError(f"cannot fork at event {at}: unfinished activations: {', '.join(sorted(state.pending))}")
        graph = _read(self.directory / "graph.json")
        overrides = fork_inputs(graph, state, inputs or {})
        correction = output_correction(graph, state, output, actor="local")
        target = type(self)(directory, models=self.models, observe=self.observe)
        location = target.directory.resolve()
        options = _read(self.directory / "options.json")
        if location.is_relative_to(self.directory.resolve()) or location.is_relative_to(self.workspace(options).resolve()):
            raise SpecError("a fork needs its own directory outside the source run and its workspace")
        try:
            target.directory.mkdir(parents=True)
        except FileExistsError:
            raise SpecError(f"{target.directory} already exists") from None
        records = journal.inspect(through=at)
        try:
            with _Lock(target.directory):
                root = next(records)
                origin = {"run_id": root["id"], "scope_id": root["scope_id"], "event": at}
                options.update(workspace=None, fork={"origin": origin, "executable": bool(execute)})
                header = {**root, "id": secrets.token_hex(8), "scope_id": secrets.token_hex(16)}
                branch = {"schema_version": 1, "kind": "fork", "origin": origin, "sent": overrides}
                saved_inputs = {}

                def prefix():
                    for record in chain([header], records, [branch], [correction] if correction else []):
                        if record["kind"] in ("run", "fork"):
                            for edge, _, _, body, *_ in record["sent"]:
                                saved_inputs[edge.removeprefix(INPUT)] = body[1]
                        yield record

                target_journal = SQLiteJournal(target.directory / journal.path.name, durable=True)
                target_journal.seed(prefix())
                state.data["run"]["id"] = header["id"]
                state.apply(branch)
                if correction:
                    state.apply(correction)
                target_journal.checkpoint(state.data)
                _write(target.directory / "options.json", options)
                _write(target.directory / "inputs.json", saved_inputs)
                target.workspace(options).mkdir()
                target._state(STOPPED, graph=graph["digest"])
                _write(target.directory / "graph.json", graph)
        except BaseException:
            shutil.rmtree(target.directory, ignore_errors=True)
            raise
        finally:
            records.close()
        return target

    def show(self, *, timeline: bool = False, at: int | None = None) -> dict:
        """Inspect the saved graph and journal without constructing or running nodes."""
        from .inspection import explain_records
        self._held()
        graph = _read(self.directory / "graph.json")
        options = _read(self.directory / "options.json", {})
        metadata = {"fork": options["fork"]} if "fork" in options else {}
        if at is not None:
            journal = self._history_journal()
            checkpoint, history = self._history_at(journal, at)
            data = history.data
            changes = data["changes"]
            graph_version = hashlib.sha256(canonical([graph["digest"], changes]).encode("utf-8")).hexdigest()
            view = explain_records(graph, journal.inspect(through=at), "historical", [], timeline=timeline)
            return {**view, **metadata, "event": at, "checkpoint": checkpoint,
                    "graph_version": {"digest": graph_version, "changes": len(changes)},
                    "values": {name: [body[1] if body[0] == "v" else
                                      {"digest": body[1], "size": body[2], "media": body[3]} for body in bodies]
                               for name, bodies in data["outputs"].items()},
                    "pending": {node: {"round": item["round"], "took": item["took"]}
                                for node, item in history.pending.items()},
                    "rework": history.rework.jobs(), "forkable": not history.pending, "ended": data["ended"]}
        state = _read(self.directory / "state.json", {})
        status = RUNNING if _holder(self.directory) else state.get("status", NEW)
        view = explain_records(graph, self._journal(options).inspect(), status, self.questions(), timeline=timeline)
        if status == FAILED:
            view["failure"] = {k: state[k] for k in ("node", "error") if k in state}
        return {**view, **metadata}

    def status(self) -> dict:
        state = _read(self.directory / "state.json")
        if state is None:
            raise SpecError(f"{self.directory} holds no run")
        if _holder(self.directory) is not None:      # being run now: what state.json holds is from before
            state = {"status": RUNNING, "graph": state.get("graph"), "updated": state.get("updated")}
        switches = self.switches()
        return {**state, **({"switches": switches} if switches else {}), "questions": self.questions()}
