"""A node that is a model: it reads what arrives, may use tools, and sends on what it concludes.

Everything the model can do outside its own reasoning is a port of the node. A tool is a call port: the
graph decides what answers it (a function, a program, a device, a person, another agent, a whole sub-graph),
so the model's reach is exactly what the graph wires up. Where the node has several outputs, the model
chooses among them.
"""
from __future__ import annotations

import base64
import copy
import inspect
import json
import typing
from pathlib import Path
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Mapping

import re

from ..base import Change, Firing, GraphError, Invalid, Node, NodeError, Port, canonical
from .function import _hints, named

_NO_PARAMETERS = {"type": "object", "properties": {}}
ANY_OUTPUT = None   # the schema of a json output: anything


@dataclass(frozen=True)
class Tool:
    """A tool the model may use. ``name`` is also the name of the call port the request leaves through;
    ``parameters`` is a JSON schema of the arguments, which arrive at the other end as one mapping.

    A tool with ``run`` is the node's own: it has no port and no edge, the node answers it itself by calling
    ``run(arguments, ctx)``. That is what a tool taken from a tool pool is (``file`` is where in the pool),
    what the file tools of a working area are, and what a plain function becomes (``Tool.of``)."""

    name: str
    description: str = ""
    parameters: Mapping = field(default_factory=lambda: dict(_NO_PARAMETERS))
    run: Callable[[Mapping, Any], Any] | None = field(default=None, compare=False)
    file: str = ""
    function: Callable | None = field(default=None, compare=False)

    @classmethod
    def of(cls, function: Callable, name: str | None = None, description: str | None = None) -> "Tool":
        """A plain function as a tool the agent answers itself, by calling it. The name is the function's,
        the description its docstring, and the arguments its parameters, typed by their annotations
        (``int``, ``float``, ``str``, ``bool``, ``list[...]``, ``dict``); those without a default are required.
        A parameter called ``ctx`` is not the model's to give: it is the agent's context."""
        if not callable(function):
            raise NodeError(f"a tool is a function, not {function!r}")
        try:
            signature = inspect.signature(function)
        except (TypeError, ValueError):
            raise NodeError(f"the parameters of {function!r} cannot be read") from None
        hints = _hints(function)
        properties: dict[str, Any] = {}
        required: list[str] = []
        takes_ctx = False
        for param in signature.parameters.values():
            if param.name == "ctx":
                takes_ctx = True
                continue
            if param.kind in (param.VAR_POSITIONAL, param.VAR_KEYWORD):
                continue
            properties[param.name] = _json_schema(hints.get(param.name))
            if param.default is param.empty:
                required.append(param.name)
        parameters: dict[str, Any] = {"type": "object", "properties": properties}
        if required:
            parameters["required"] = required

        def run(arguments: Mapping, ctx: Any) -> Any:
            unknown = set(arguments) - set(properties)
            if unknown:
                raise NodeError(f"no argument {', '.join(sorted(unknown))}")
            return function(**arguments, **({"ctx": ctx} if takes_ctx else {}))
        title = name or getattr(function, "__name__", "")
        if not title or title == "<lambda>":
            raise NodeError("a tool made of a function needs a name: give one")
        about = description if description is not None else inspect.cleandoc(function.__doc__ or "")
        return cls(title, about, parameters, run=run, function=function)

    def spec(self) -> dict:
        return {"name": self.name, "description": self.description, "parameters": dict(self.parameters)}

    def describe(self) -> dict:
        """The tool as a graph's description records it."""
        if self.function is not None:
            return {**self.spec(), "function": named(self.function)}
        return {**self.spec(), **({"file": self.file} if self.file else {"own": True} if self.run is not None else {})}


def _json_schema(hint: Any) -> dict:
    """The JSON schema of an argument, from its annotation; anything when it says nothing plain."""
    simple = {int: "integer", float: "number", str: "string", bool: "boolean", dict: "object", list: "array"}
    if hint in simple:
        return {"type": simple[hint]}
    origin, args = typing.get_origin(hint), typing.get_args(hint)
    if origin is list:
        return {"type": "array", **({"items": _json_schema(args[0])} if args else {})}
    if origin is dict:
        return {"type": "object"}
    return {}


def time_left(ctx) -> str:
    """How much time you have left before your time limit, in seconds."""
    left = ctx.time_left()
    return "you have no time limit" if left is None else f"{left:.0f} seconds"


_RESIDENT = {
    "wait": {"description": "Wait for the next message from the others (they reach you through your inputs). "
                            "Returns what came, with your task board; or says that nothing more can come.",
             "parameters": {"type": "object", "properties": {"seconds": {"type": "number", "description":
                            "give up after this long; leave out to wait as long as it takes"}}}},
    "note": {"description": "Write down where you stand, for yourself. It is kept, and your progress is saved.",
             "parameters": {"type": "object", "properties": {"text": {"type": "string"}}, "required": ["text"]}},
    "plan": {"description": "Add items to your task board.",
             "parameters": {"type": "object", "properties": {"items": {"type": "array", "items": {"type": "string"}}},
                            "required": ["items"]}},
    "done": {"description": "Tick off an item of your task board, by its number or its text.",
             "parameters": {"type": "object", "properties": {"item": {"type": ["string", "integer"]}}, "required": ["item"]}},
    "correct": {"description": "Correct a message you sent: give its id (what sending it answered) and the content "
                               "that is to take its place. Whoever has not read it yet gets only the correction; "
                               "whoever has, is told.",
                "parameters": {"type": "object", "properties": {"id": {"type": "string"}, "content": {}},
                               "required": ["id", "content"]}},
    "explain": {"description": "Answer a rejection of something you sent, when it is right as it is: give the "
                               "rejection's id and why. (When it is wrong, correct it instead.)",
                "parameters": {"type": "object", "properties": {"id": {"type": "string"}, "text": {"type": "string"}},
                               "required": ["id", "text"]}},
    "finish": {"description": "Say that your work is done. Send everything first: what you have not sent is lost.",
               "parameters": {"type": "object", "properties": {"summary": {"type": "string"}}}},
}


_CORRECTS = ": this corrects what came here before"

#: What an agent is told about a message it may reject, unless it is given its own words (``doubt``).
DOUBT = ("Before you use what came on {ports}, judge whether it is what you expected. If it is not, reject it with "
         "the reject tool and say why: its sender will explain it or correct it.")
_REJECT = {"name": "reject", "description": "Reject what came on one of your inputs because it is not what you "
                                            "expected, and wait for its sender to explain or correct it.",
           "parameters": {"type": "object", "properties": {
               "port": {"type": "string"}, "reason": {"type": "string"},
               "which": {"type": "string", "description": "on an input fed by several: who sent the one you reject "
                                                          "(the last one when left out)"}},
               "required": ["port", "reason"]}}
_ANSWER = [{"name": "explain", "description": "It is right as it is: say why.",
            "parameters": {"type": "object", "properties": {"text": {"type": "string"}}, "required": ["text"]}},
           {"name": "correct", "description": "It is wrong: give what is to take its place. Give the id of another "
                                              "message you sent to correct that one too.",
            "parameters": {"type": "object", "properties": {"content": {}, "id": {"type": "string"}},
                           "required": ["content"]}}]


_HIRING = {
    "hire": {"description": "Hire a member for your team from one of your templates. You send it work with "
                            "send_to_<name>, and what it sends back comes to you on from_<name>. You manage it.",
             "parameters": {"type": "object", "properties": {
                 "template": {"type": "string"}, "name": {"type": "string", "description": "letters, digits, _"},
                 "task": {"description": "work to send it at once (optional)"}}, "required": ["template", "name"]}},
    "dismiss": {"description": "Dismiss a member of your team, and those it manages. It is stopped at once "
                               "and taken out once it has ended.",
                "parameters": {"type": "object", "properties": {"name": {"type": "string"}}, "required": ["name"]}},
    "control": {"description": "Pause, resume, stop or restart a member of your team (and those it manages).",
                "parameters": {"type": "object", "properties": {
                    "name": {"type": "string"},
                    "action": {"type": "string", "enum": ["pause", "resume", "stop", "restart"]}},
                    "required": ["name", "action"]}},
    "team": {"description": "Who is in your team, and how each stands.",
             "parameters": {"type": "object", "properties": {}}},
    "fail": {"description": "End the whole run as failed, for this reason: when a member's failure, or anything "
                            "else, means the work cannot be done.",
             "parameters": {"type": "object", "properties": {"reason": {"type": "string"}}, "required": ["reason"]}},
    "connect": {"description": "Connect an output of one member of your team to an input of another, so that what "
                               "the first sends goes to the second. Write each end as name.port; you are 'me'. "
                               "Say back=true for a connection that closes a loop.",
                "parameters": {"type": "object", "properties": {
                    "source": {"type": "string"}, "target": {"type": "string"}, "back": {"type": "boolean"}},
                    "required": ["source", "target"]}},
}
_NAME = re.compile(r"[A-Za-z0-9_]{1,32}")


def _doubtful(ctx, ports) -> list:
    """The input ports whose last message this activation may still reject."""
    challengeable = getattr(ctx, "challengeable", None)
    return [port for port in ports if challengeable(port)] if challengeable is not None else []


def _verdict(got: Mapping) -> str:
    if got.get("answer") == "explained":
        return f"Its sender explains, and it stands: {got.get('text', '')}"
    if got.get("answer") == "corrected":
        return f"Its sender corrected it. It is now:\n{_text(got.get('value'))}"
    return f"Not answered: {got.get('why', '')}"


def _rejected(rejections: list) -> str:
    return "\n\n".join(
        f"[rejected] What you sent as {r['message'] or 'a message'} on {r['port']} was rejected by {r['by']}: "
        f"{r['reason']}\nWhat they got:\n{_text(r['content'])}\nAnswer with explain(id={r['id']!r}, text) if it is "
        f"right, or correct(id={r['message']!r}, content) if it is not." + _siblings(r) for r in rejections)


def _siblings(rejection: Mapping) -> str:
    """What else was sent with the rejected message, which may be wrong for the same reason."""
    sent = rejection.get("sent") or {}
    if not sent:
        return ""
    listed = "; ".join(f"{', '.join(names)} on {port}" for port, names in sent.items())
    return (f"\nWith it you also sent {listed}. If any of those is wrong for the same reason, correct it too "
            f"(correct with its id), before you answer this.")


def _corrected(ctx, got: Mapping[str, Any]) -> set:
    """The ports among ``got`` whose message corrects one read before."""
    replaces = getattr(ctx, "replaces", None)
    return {name for name in got if replaces(name)} if replaces is not None else set()


def _step(ctx) -> bool:
    """A step is done: wait while paused; False when the activation is to end now."""
    checkpoint = getattr(ctx, "checkpoint", None)
    return checkpoint() if checkpoint is not None else not ctx.stopping()


def _save(ctx) -> None:
    commit = getattr(ctx, "commit", None)
    if commit is not None:
        commit()


def is_image(value: Any) -> bool:
    """A picture, as it travels on an edge: ``{"mime": "image/png", "data": <base64 text>}``."""
    return (isinstance(value, Mapping) and isinstance(value.get("data"), str)
            and isinstance(value.get("mime"), str) and value["mime"].startswith("image/"))


def _text(value: Any) -> str:
    if isinstance(value, str):
        return value
    try:
        return canonical(value)
    except (TypeError, ValueError):
        return repr(value)


class AgentNode(Node):
    """``model`` is anything with ``chat(system, messages, tools, max_tokens)`` that returns an object with
    ``text`` and ``tool_calls`` (a list of ``{"id", "name", "args"}``); messages are
    ``{"role": "user" | "assistant" | "tool", "content": ...}``. Its ``name`` (or else its ``model``) attribute is
    what the node's description records as the model, and what a catalog is asked for when the graph is loaded.

    One activation is one turn: what arrived becomes a user message, the model is asked, every tool it calls
    is answered through the call port of that name, and it is asked again until it calls nothing more.

    * With one output, the model's closing text is sent there (parsed as JSON when ``json=True``).
    * With several, nothing is sent by itself: the model is given a tool ``send_<port>`` for each output and
      decides what goes where, and when. This is how an agent coordinates others.

    A tool that fails answers the model with the error; the model decides what to do about it. With
    ``memory`` (the default) the conversation carries over from one activation to the next and is part of the
    node's snapshot. ``memory`` may also be a function ``memory(messages) -> messages``: it is called before
    every request to the model and what it returns becomes the conversation, so it is where a long
    conversation is shortened, summarised or otherwise kept in hand. ``usage`` adds up the tokens the model
    reports.

    A picture that arrives (``{"mime": "image/...", "data": <base64>}``, alone or inside what arrived) is
    shown to the model as a picture, where the model's interface takes pictures. With ``files`` (a
    directory), a picture that arrives as a file reference (``{"path": "plots/a.png", "media": "image/png"}``,
    as the ``file`` protocol delivers it) is read from there and shown too.

    What the model sends is checked like any node's output. When a check finds a problem and the port has
    repairs left (an agent's ports have two unless they say otherwise), the model is told what is wrong, in
    the port's own words, and answers again; it still has its tools, so it can write the file that was
    missing. The same goes for an answer that should have been JSON and is not.

    With ``area`` (an ``Area`` of a ``Workspace``), a file the node sends out of a port that carries files
    must be one the area lets it read; one that is not goes back to the model with the reason.

    With ``resident=True`` the agent stays for as long as it has work, and decides itself when it is done:
    one activation, not one per message. It sends each output when it is ready (``send_<port>``, even with
    one output), waits for messages from the others (``wait``, which holds none of the run's workers),
    keeps a task board (one line per output, plus what it adds with ``plan`` and ticks off with ``done``),
    writes down where it stands (``note``, which also saves its progress to the journal) and ends with
    ``finish``. ``wait`` says so when nothing more can come, because everyone else has finished or waits
    too. Taken up after the process ended, it goes on from its last saved progress.
    """

    kind = "agent"
    repairs = 2
    on_correction = "tell"           # a model reads the correction and decides what to do about it

    def __init__(self, id: str, model: Any, *, system: str = "", tools: Iterable[Tool] = (),
                 inputs: Iterable[Port] | None = None, outputs: Iterable[Port] | None = None,
                 firing: Firing | None = None, memory: bool | Callable[[list], list] = True, json: bool = False, max_steps: int = 50,
                 max_tokens: int = 4096, files=None, repairs: int | None = None, area=None, resident: bool = False,
                 doubt: str | None = None, hire: int = 0, templates: Mapping[str, Any] | None = None,
                 hire_rounds: int = 10, builder: Callable[[Mapping], Node] | None = None):
        if not callable(getattr(model, "chat", None)):
            raise NodeError(f"{id}: a model needs a chat method")
        if type(max_steps) is not int or max_steps < 1:
            raise NodeError(f"{id}: max_steps must be at least 1")
        tools = tuple(tool if isinstance(tool, Tool) else Tool.of(tool) for tool in tools)
        if len({tool.name for tool in tools}) != len(tools):
            raise NodeError(f"{id}: two tools have the same name")
        sends = list(outputs) if outputs is not None else [Port("out", ANY_OUTPUT if json else str)]
        if any(port.call for port in sends):
            raise NodeError(f"{id}: declare what the model may call as tools, not as outputs")
        super().__init__(id, inputs=list(inputs) if inputs is not None else [Port("task")],
                         outputs=[*sends, *(Port(tool.name, call=True) for tool in tools if tool.run is None)], firing=firing)
        self.model, self.system, self.tools = model, system, tools
        self.keep = memory if callable(memory) else None       # what is done to the conversation before each request
        self.memory, self.json, self.max_steps, self.max_tokens = bool(memory), bool(json), max_steps, max_tokens
        self.files, self.area = files, area
        self.doubt = doubt                  # what it is told about a message it may reject; None: DOUBT
        if isinstance(hire, bool) or type(hire) is not int or hire < 0:
            raise NodeError(f"{id}: hire is how many it may hire, a whole number (0: none)")
        if type(hire_rounds) is not int or hire_rounds < 1:
            raise NodeError(f"{id}: hire_rounds must be at least 1")
        self.hire, self.hire_rounds = hire, hire_rounds
        self.oversees = bool(hire)          # a member it hired that fails is its to deal with, not the run's end
        self.templates = dict(templates or {})
        self._builder = builder
        if hire and not resident:
            raise NodeError(f"{id}: only a resident agent may hire (it stays to hear back from its team)")
        if hire and not self.templates:
            raise NodeError(f"{id}: an agent that may hire needs templates to hire from")
        self.hires = 0                      # how many it has hired in this run: hire is the most
        self.hired: dict[str, str] = {}     # the members it has now: name -> node id
        self.leaving: dict[str, list] = {}  # dismissed, stopped, not yet taken out: name -> the ids to take out
        self._extra: set[str] = set()       # output ports given to it while the run went on (one per member)
        if any(tool.name in ("reject", "explain") for tool in tools):
            raise NodeError(f"{id}: the tools reject and explain are the agent's own")
        self._own = {tool.name: tool for tool in tools if tool.run is not None}
        self._paths = {port.name for port in sends if isinstance(port.schema.describe(), Mapping) and "file" in port.schema.describe()}
        if repairs is not None:
            if type(repairs) is not int or repairs < 0:
                raise NodeError(f"{id}: repairs must be a whole number, 0 or more")
            self.repairs = repairs
        self.messages: list[dict] = []
        self.usage = {"calls": 0, "input_tokens": 0, "output_tokens": 0}
        self._sends = [port.name for port in sends]
        self.resident = bool(resident)
        self._choose = len(self._sends) > 1 or self.resident
        self._specs = [tool.spec() for tool in tools]
        self.board: dict = self._fresh_board()
        if self.resident:
            taken = {tool.name for tool in tools} & set(_RESIDENT)
            if taken:
                raise NodeError(f"{id}: the tools {', '.join(sorted(taken))} are a resident agent's own")
            self._specs += [{"name": name, **spec} for name, spec in _RESIDENT.items()]
        if self.hire:
            taken = {tool.name for tool in tools} & set(_HIRING)
            if taken:
                raise NodeError(f"{id}: the tools {', '.join(sorted(taken))} are those of an agent that hires")
            self._specs += [{"name": name, **spec} for name, spec in _HIRING.items()]
        if self._choose:
            if any(tool.name.startswith("send_") for tool in tools):
                raise NodeError(f"{id}: tool names beginning with send_ are taken by the node's outputs")
            self._specs += [{"name": f"send_{name}", "description": f"Send content out of the output '{name}'.",
                             "parameters": {"type": "object", "properties": {"content": {}}, "required": ["content"]}}
                            for name in self._sends]

    def config(self) -> Mapping:
        return {"model": str(getattr(self.model, "name", "") or getattr(self.model, "model", "")), "system": self.system,
                "tools": [tool.describe() if tool.run is not None else tool.spec() for tool in self.tools], "memory": named(self.keep) if self.keep is not None else self.memory, "json": self.json,
                "max_steps": self.max_steps, "max_tokens": self.max_tokens, "repairs": self.repairs,
                **({"resident": True} if self.resident else {}),
                **({"doubt": self.doubt} if self.doubt is not None else {}),
                **({"hire": self.hire, "hire_rounds": self.hire_rounds,
                    "templates": {name: t if isinstance(t, Mapping) else named(t) for name, t in self.templates.items()}}
                   if self.hire else {}),
                **(self.area.given if self.area is not None else {})}

    def reset(self) -> None:
        self.messages = []
        self.board = self._fresh_board()

    def snapshot(self) -> Mapping:
        kept: dict[str, Any] = {"messages": self.messages, "usage": self.usage} if self.memory else {"usage": self.usage}
        if self.hire:
            kept = {**kept, "hires": self.hires, "hired": dict(self.hired),
                    **({"leaving": dict(self.leaving)} if self.leaving else {})}
        return {**kept, "board": self.board} if self.resident else kept

    def restore(self, state: Mapping) -> None:
        self.messages = list(state.get("messages", []))
        self.usage = dict(state.get("usage", self.usage))
        self.hires, self.hired = int(state.get("hires", self.hires)), dict(state.get("hired", self.hired))
        self.leaving = {name: list(ids) for name, ids in state.get("leaving", {}).items()}
        if "board" in state:
            self.board = {**self._fresh_board(), **copy.deepcopy(dict(state["board"]))}

    def add_ports(self, inputs: Iterable[Port] = (), outputs: Iterable[Port] = ()) -> None:
        """Ports it was given while the run went on: each new output is one more thing it can send."""
        outputs = list(outputs)
        super().add_ports(inputs, outputs)
        for port in outputs:
            self._sends.append(port.name)
            self._extra.add(port.name)
            self.board["sent"].setdefault(port.name, 0)
            self._specs.append({"name": f"send_{port.name}", "description": f"Send content out of the output "
                                f"'{port.name}'.", "parameters": {"type": "object", "properties": {"content": {}},
                                                                  "required": ["content"]}})

    def drop_ports(self, names: Iterable[str]) -> None:
        names = set(names)
        super().drop_ports(names)
        self._sends = [name for name in self._sends if name not in names]
        self._extra -= names
        self._specs = [spec for spec in self._specs if spec["name"] not in {f"send_{name}" for name in names}]

    # ---- a team of its own ------------------------------------------------------------------------------

    def _member(self, template: Any, id: str) -> Node:
        if callable(template) and not isinstance(template, Mapping):
            return template(id)
        described = {**template, "id": id}
        if self.area is not None and "layer" not in described:
            described["layer"] = self.area.node.split("/")       # its directory is inside the one of who hired it
        if self._builder is None:
            raise NodeError("a template written as a description needs builder= (a graph file gives it one); "
                            "or give a function of the id that makes the node")
        return self._builder(described)

    def remake(self, template: str, id: str) -> Node:
        """A member it hired, made again from the same template when a journaled run is taken up."""
        member = self._member(self.templates[template], id)
        if isinstance(member, AgentNode) and member.hire > self.hire:
            member.hire = self.hire
        member.made_by = {"node": self.id, "template": template}
        return member

    def _hire(self, ctx, args: Mapping) -> str:
        name, template = str(args.get("name", "")), str(args.get("template", ""))
        if self.hires >= self.hire:
            return f"error: you have hired all you may ({self.hire})"
        if template not in self.templates:
            return f"error: no template {template!r}; you have {', '.join(sorted(self.templates))}"
        if not _NAME.fullmatch(name) or name in self.hired or name in self.leaving or f"to_{name}" in self.outputs:
            return f"error: {name!r} is taken or is not a name (letters, digits, _)"
        id = f"{self.id}-{name}"
        try:
            member = self._member(self.templates[template], id)
        except Exception as exc:   # noqa: BLE001 - the model's to read
            return f"error: the template {template} cannot be made: {exc}"
        if isinstance(member, AgentNode) and member.hire > self.hire:
            member.hire = self.hire                  # it may hire no more than who hired it
        member.made_by = {"node": self.id, "template": template}
        takes = [p for p in member.inputs.values() if p.required] or list(member.inputs.values())
        gives = [p for p in member.outputs.values() if not p.call]
        if not takes:
            return f"error: a {template} takes nothing, so it cannot be given work"
        change = (Change().add(member).ports(self.id, inputs=[f"from_{name}"] if gives else [], outputs=[f"to_{name}"])
                  .connect(f"{self.id}.to_{name}", f"{id}.{takes[0].name}").manage(self.id, id))
        if gives:
            change.connect(f"{id}.{gives[0].name}", f"{self.id}.from_{name}", back=True, max_rounds=self.hire_rounds)
        try:
            ctx.change(change)
        except (GraphError, NodeError) as exc:
            return f"error: {exc}"
        self.hires += 1
        self.hired[name] = id
        told = f"hired {name}: send it work with send_to_{name}" + (f"; what it sends comes to you on from_{name}"
                                                                    if gives else "")
        if args.get("task") is not None:
            told += "; " + self._use(ctx, f"send_to_{name}", {"content": args["task"]})
        return told + f". You may hire {self.hire - self.hires} more."

    def _dismiss(self, ctx, args: Mapping) -> str:
        name = str(args.get("name", ""))
        id = self.hired.get(name)
        if id is None:
            return f"error: {name!r} is not in your team"
        team = ctx.team()
        below = [member for member in team if self._under(team, member, id)]
        try:
            ctx.control([id], "stop", team=True)
        except (GraphError, NodeError) as exc:
            return f"error: {exc}"
        del self.hired[name]
        told = f"dismissed {name}" + (f" and {len(below)} under it" if below else "")
        self.leaving[name] = [*reversed(below), id]
        if self._tidy(ctx):
            return told + " (it is stopping, and is taken out of the graph once its work has ended)"
        return told

    def _tidy(self, ctx) -> list:
        """Take out of the graph the members it dismissed that have stopped working; the names still there."""
        for name, ids in list(self.leaving.items()):
            try:
                ctx.change(Change().remove(*ids))
            except (GraphError, NodeError):
                continue                     # one is still running: tried again before the next model call
            del self.leaving[name]
        return list(self.leaving)

    def _failed(self, failures: list) -> str:
        names = {id: name for name, id in self.hired.items()}
        return "\n\n".join(
            f"[failed] {names.get(f['node'], f['node'])} failed in its round {f['round']}: {f['error']}\nIt is "
            f"stopped. Restart it with control(name={names.get(f['node'], f['node'])!r}, action='restart'), dismiss "
            f"it, or call fail(reason) if the work cannot be done without it." for f in failures)

    @staticmethod
    def _under(team: Mapping, member: str, top: str) -> bool:
        above = team.get(member, {}).get("manager")
        while above is not None:
            if above == top:
                return True
            above = team.get(above, {}).get("manager")
        return False

    def _whose(self, name: str) -> str | None:
        head, slash, rest = name.partition("/")
        id = self.hired.get(head, head if head.startswith(f"{self.id}-") else None)
        return None if id is None else id + slash + rest

    def _control_member(self, ctx, args: Mapping) -> str:
        id = self._whose(str(args.get("name", "")))
        if id is None:
            return f"error: {args.get('name')!r} is not in your team"
        try:
            changed = ctx.control([id], str(args.get("action", "")), team=True)
        except Exception as exc:   # noqa: BLE001
            return f"error: {exc}"
        return f"{args.get('action')}: {', '.join(changed)}" if changed else "nothing changed"

    def _team(self, ctx) -> str:
        team = ctx.team()
        if not team:
            return "You have no team." + (f" You may hire {self.hire - self.hires}." if self.hire else "")
        names = {id: name for name, id in self.hired.items()}
        leaving = {ids[-1]: name for name, ids in self.leaving.items()}
        names.update(leaving)
        lines = [f"- {names.get(id, id)}: {about['status']}" + (" (dismissed, being taken out)" if id in leaving else "")
                 + (", waiting" if about["waiting"] else ", working" if about["running"] else "")
                 + f", {about['rounds']} round{'s' if about['rounds'] != 1 else ''}"
                 + ("" if about["manager"] == self.id else f", under {names.get(about['manager'], about['manager'])}")
                 for id, about in team.items()]
        return "\n".join(["Your team:", *lines, f"You may hire {self.hire - self.hires} more."])

    def _connect(self, ctx, args: Mapping) -> str:
        ends = []
        team = set(ctx.team()) | {self.id}
        for end in (str(args.get("source", "")), str(args.get("target", ""))):
            who, dot, port = end.partition(".")
            id = self.id if who == "me" else self.hired.get(who, who)
            if not dot or id not in team:
                return f"error: {end!r} is not name.port of you ('me') or a member of your team"
            ends.append(f"{id}.{port}")
        back = bool(args.get("back"))
        try:
            ctx.change(Change().connect(*ends, back=back, max_rounds=self.hire_rounds if back else None))
        except (GraphError, NodeError) as exc:
            return f"error: {exc}"
        return f"connected {args.get('source')} to {args.get('target')}"

    def _fresh_board(self) -> dict:
        return {"sent": {name: 0 for name in self._sends}, "fixed": {}, "items": [], "notes": [], "warned": False}

    # ---- one turn --------------------------------------------------------------------------------------

    def run(self, ctx, inputs: Mapping[str, Any]):
        if self.resident:
            return self._stay(ctx, inputs)
        pictures: list = []
        inputs = {name: self._pictures(value, pictures) for name, value in inputs.items()}
        if len(inputs) == 1 and len(self.inputs) == 1 and not _corrected(ctx, inputs):
            arrived = _text(next(iter(inputs.values()))) + self._doubting(ctx, inputs)
        else:
            arrived = self._heard(inputs, ctx)
        # the conversation is taken up on a copy and kept only when the turn ends well: a turn that fails or is
        # stopped half way leaves nothing behind, so running it again starts from where the last good turn ended
        messages = list(self.messages) if self.memory else []
        messages.append({"role": "user", "content": arrived, **({"image_data": pictures} if pictures else {})})
        for problem in ctx.problems:
            messages.append({"role": "user", "content": f"What you sent was not accepted: {problem}"})
        unparsed = self.repairs
        for _ in range(self.max_steps):
            if not _step(ctx):             # paused: waits here; stopped, restarted or the run is ending: leaves
                return None
            if self.keep is not None:
                kept = self.keep(messages)
                if not isinstance(kept, list) or not kept:
                    raise NodeError("memory must return the conversation: a list of messages that is not empty")
                messages[:] = kept
            result = self.model.chat(self.system, messages, self._tools(ctx), self.max_tokens)
            spent_in, spent_out = getattr(result, "input_tokens", 0) or 0, getattr(result, "output_tokens", 0) or 0
            self.usage["calls"] += 1
            self.usage["input_tokens"] += spent_in
            self.usage["output_tokens"] += spent_out
            ctx.use(calls=1, input_tokens=spent_in, output_tokens=spent_out, tokens=spent_in + spent_out)
            text, calls = getattr(result, "text", "") or "", list(getattr(result, "tool_calls", None) or ())
            message: dict[str, Any] = {"role": "assistant", "content": text}
            if calls:
                message["tool_calls"] = [{"id": call.get("id", ""), "name": call["name"], "args": call.get("args") or {}}
                                         for call in calls]
            thought = getattr(result, "thinking_blocks", None)
            if thought:     # a service that thinks between tool calls wants its thinking back with the next request
                message["thinking_blocks"] = list(thought)
            messages.append(message)
            if not calls:
                if self._choose:
                    self.messages = messages if self.memory else []
                    return None
                if not _step(ctx):         # paused while the model answered: the answer waits; stopped: it is not sent
                    return None
                try:
                    try:
                        answer = self._closing(text)
                    except NodeError as exc:
                        if unparsed <= 0:
                            raise
                        unparsed -= 1
                        raise Invalid(self._sends[0], str(exc), again=True) from None
                    self._send(ctx, self._sends[0], answer)
                    self.messages = messages if self.memory else []
                    return None
                except Invalid as exc:
                    if not exc.again:
                        raise
                    messages.append({"role": "user", "content": f"Your answer was not accepted: {exc.problem}\n"
                                                                f"Put that right and give your answer again."})
                    continue
            for call in message["tool_calls"]:
                if not _step(ctx):
                    return None
                messages.append({"role": "tool", "tool_call_id": call["id"], "name": call["name"],
                                 "content": self._use(ctx, call["name"], call["args"])})
        raise NodeError(f"the model was still calling tools after {self.max_steps} steps")

    def challenged(self, ctx, rejection: Mapping) -> Any:
        """Something it sent was rejected while it was not there to see it: the model is asked to explain or
        correct, with its conversation (when it keeps one)."""
        messages = list(self.messages) if self.memory else []
        messages.append({"role": "user", "content": _rejected([dict(rejection)]).replace(
            f"explain(id={rejection['id']!r}, text)", "explain(text)").replace(
            f"correct(id={rejection['message']!r}, content)", "correct(content)")})
        for _ in range(self.max_steps):
            message, calls = self._ask(ctx, messages, _ANSWER)
            if not calls:
                self.messages = messages if self.memory else []
                return message.get("content") or "no reason was given"
            answered: tuple | None = None       # ("explained", text) or ("corrected",), once the rejected one is
            for call in message["tool_calls"]:
                name, args = call["name"], call["args"]
                if name == "explain" and answered is None:
                    answer, answered = "answered", ("explained", str(args.get("text", "")))
                elif name == "correct" and rejection.get("message"):
                    id = str(args.get("id") or rejection["message"])
                    answer = self._fix(ctx, {"id": id, "content": args.get("content")})
                    if id == rejection["message"] and answer.startswith("corrected"):
                        answered = ("corrected",)
                else:
                    answer = "error: answer with explain or correct"
                messages.append({"role": "tool", "tool_call_id": call["id"], "name": name, "content": answer})
            if answered is not None:
                self.messages = messages if self.memory else []
                return answered[1] if answered[0] == "explained" else None
        raise NodeError(f"the model did not answer the rejection in {self.max_steps} steps")

    # ---- a resident agent -------------------------------------------------------------------------------

    def _stay(self, ctx, inputs: Mapping[str, Any]):
        """One activation for as long as the agent has work: it ends when the agent says it has finished."""
        messages = list(self.messages) if self.memory else []
        if getattr(ctx, "resumed", False):
            messages.append({"role": "user", "content": "The run was taken up again after an interruption. "
                                                        "Here is where you stood:\n" + self._shown_board()})
        else:
            self.board = self._fresh_board()
            messages.append(self._arrival(inputs, ctx) if inputs else
                            {"role": "user", "content": "You have started. " + self._shown_board()})
        steps = 0
        while True:
            if not _step(ctx):
                self.messages = messages if self.memory else []
                return None
            if steps >= self.max_steps:
                raise NodeError(f"the model went {self.max_steps} steps without waiting or finishing")
            steps += 1
            if self.leaving:
                self._tidy(ctx)
            if self.keep is not None:
                kept = self.keep(messages)
                if not isinstance(kept, list) or not kept:
                    raise NodeError("memory must return the conversation: a list of messages that is not empty")
                messages[:] = kept
            message, calls = self._ask(ctx, messages)
            if not calls:
                messages.append({"role": "user", "content": "Use your tools: wait for messages, send what is "
                                                            "ready, or finish when your work is done."})
                continue
            for call in message["tool_calls"]:
                if not _step(ctx):
                    self.messages = messages if self.memory else []
                    return None
                name, args = call["name"], call["args"]
                if name == "finish":
                    unsent = [port for port, count in self.board["sent"].items()
                              if not count and port not in self._extra]
                    if unsent and not self.board["warned"]:
                        self.board["warned"] = True
                        answer = (f"Nothing has been sent out of {', '.join(unsent)}. Send it, or call finish "
                                  f"again to end without it.")
                    else:
                        messages.append({"role": "tool", "tool_call_id": call["id"], "name": name, "content": "finished"})
                        self.messages = messages if self.memory else []
                        return None
                elif name == "wait":
                    self.messages = list(messages) if self.memory else []
                    _save(ctx)                   # what it has done so far is kept while it waits
                    got = ctx.receive(timeout=args.get("seconds"))
                    if got is not None:
                        steps = 0
                        rejections = ctx.rejections() if hasattr(ctx, "rejections") else []
                        failed = ctx.failures() if self.oversees and hasattr(ctx, "failures") else []
                        answer = "\n\n".join(part for part in (self._heard(got, ctx) if got else "",
                                                                 _rejected(rejections), self._failed(failed),
                                                                 self._shown_board()) if part)
                    elif ctx.stopping():
                        answer = "The run is ending."
                    elif args.get("seconds") is not None:
                        answer = "Nothing came in that time.\n\n" + self._shown_board()
                    else:
                        answer = ("Nothing more can come: everyone else has finished or is waiting too. Send "
                                  "what is left, then finish.\n\n" + self._shown_board())
                elif name == "note":
                    self.board["notes"].append(str(args.get("text", "")))
                    messages.append({"role": "tool", "tool_call_id": call["id"], "name": name, "content": "noted"})
                    self.messages = list(messages) if self.memory else []
                    _save(ctx)
                    continue
                elif name == "plan":
                    items = args.get("items") or []
                    self.board["items"] += [[str(item), False] for item in (items if isinstance(items, list) else [items])]
                    answer = self._shown_board()
                elif name == "done":
                    answer = self._tick(args.get("item"))
                elif name == "correct":
                    answer = self._fix(ctx, args)
                elif name in _HIRING and self.hire:
                    if name == "fail":
                        raise NodeError(f"{self.id} gave up: {args.get('reason', '')}")
                    answer = {"hire": self._hire, "dismiss": self._dismiss, "control": self._control_member,
                              "team": lambda ctx, args: self._team(ctx), "connect": self._connect}[name](ctx, args)
                elif name == "explain":
                    try:
                        ctx.explain(str(args.get("id", "")), str(args.get("text", "")))
                        answer = "answered"
                    except Exception as exc:   # noqa: BLE001 - the model's to read
                        answer = f"error: {exc}"
                else:
                    answer = self._use(ctx, name, args)
                messages.append({"role": "tool", "tool_call_id": call["id"], "name": name, "content": answer})

    def _ask(self, ctx, messages: list, specs: list | None = None) -> tuple[dict, list]:
        """One request to the model; its answer is added to ``messages``."""
        result = self.model.chat(self.system, messages, specs if specs is not None else self._tools(ctx),
                                 self.max_tokens)
        spent_in, spent_out = getattr(result, "input_tokens", 0) or 0, getattr(result, "output_tokens", 0) or 0
        self.usage["calls"] += 1
        self.usage["input_tokens"] += spent_in
        self.usage["output_tokens"] += spent_out
        ctx.use(calls=1, input_tokens=spent_in, output_tokens=spent_out, tokens=spent_in + spent_out)
        text, calls = getattr(result, "text", "") or "", list(getattr(result, "tool_calls", None) or ())
        message: dict[str, Any] = {"role": "assistant", "content": text}
        if calls:
            message["tool_calls"] = [{"id": call.get("id", ""), "name": call["name"], "args": call.get("args") or {}}
                                     for call in calls]
        thought = getattr(result, "thinking_blocks", None)
        if thought:
            message["thinking_blocks"] = list(thought)
        messages.append(message)
        return message, calls

    def _arrival(self, inputs: Mapping[str, Any], ctx=None) -> dict:
        pictures: list = []
        inputs = {name: self._pictures(value, pictures) for name, value in inputs.items()}
        if len(inputs) == 1 and len(self.inputs) == 1 and not _corrected(ctx, inputs):
            arrived = _text(next(iter(inputs.values()))) + self._doubting(ctx, inputs)
        else:
            arrived = self._heard(inputs, ctx)
        return {"role": "user", "content": arrived, **({"image_data": pictures} if pictures else {})}

    def _heard(self, got: Mapping[str, Any], ctx=None) -> str:
        text = "\n\n".join(f"[{name}{_CORRECTS if name in _corrected(ctx, got) else ''}]\n{_text(value)}"
                             for name, value in got.items())
        return text + self._doubting(ctx, got)

    def _doubting(self, ctx, got: Mapping[str, Any]) -> str:
        doubtful = _doubtful(ctx, got)
        if not doubtful:
            return ""
        text = "\n\n" + (self.doubt if self.doubt is not None else DOUBT).format(ports=", ".join(doubtful))
        taken = getattr(ctx, "taken", None)
        if taken is None:
            return text
        for port in doubtful:
            senders = [one["from"] or one["edge"] for one in taken(port)]
            if len(senders) > 1:
                text += (f"\nWhat came on {port} came from {', '.join(senders)}, in that order: to reject one of "
                         f"them, say which=<who sent it>.")
        return text

    def _tools(self, ctx) -> list:
        """The tools for one request: ``reject`` while something it took may still be rejected."""
        return [*self._specs, _REJECT] if _doubtful(ctx, self.inputs) else self._specs

    def _fix(self, ctx, args: Mapping) -> str:
        """The ``correct`` tool: put new content in the place of a message the agent sent."""
        sent, port = str(args.get("id", "")), str(args.get("id", "")).rpartition("#")[0]
        if "content" not in args:
            return "error: content is missing"
        if port not in self._sends:
            return f"error: {sent!r} is not the id of a message you sent"
        try:
            if self.area is not None and port in self._paths:
                problem = self.area.sends(args["content"])
                if problem:
                    return f"not accepted: {problem}. Put that right and correct it again."
            ctx.correct(sent, args["content"])
        except Invalid as exc:
            if not exc.again:
                raise
            return f"not accepted: {exc.problem}. Put that right and correct it again."
        except Exception as exc:   # noqa: BLE001 - whatever went wrong is the model's to read
            if ctx.stopping():
                raise
            return f"error: {exc}"
        fixed = self.board.setdefault("fixed", {})
        fixed[port] = fixed.get(port, 0) + 1
        return f"corrected {sent}"

    def _shown_board(self) -> str:
        board = self.board
        lines = ["Your task board:"]
        fixed = board.get("fixed", {})
        lines += [f"- output {port}: " + (f"sent {count} time{'s' if count > 1 else ''}" if count else "not sent yet")
                  + (f", {fixed[port]} correction{'s' if fixed[port] > 1 else ''}" if fixed.get(port) else "")
                  for port, count in board["sent"].items()]
        lines += [f"{i}. [{'x' if done else ' '}] {text}" for i, (text, done) in enumerate(board["items"], 1)]
        if board["notes"]:
            lines.append(f"Your last note: {board['notes'][-1]}")
        return "\n".join(lines)

    def _tick(self, item: Any) -> str:
        items = self.board["items"]
        for i, entry in enumerate(items, 1):
            if item == i or str(item).strip() in (str(i), entry[0]):
                entry[1] = True
                return self._shown_board()
        return f"error: there is no item {item!r} on your board\n\n" + self._shown_board()

    def _pictures(self, value: Any, found: list) -> Any:
        """Take the pictures out of what arrived, leaving a note where each one was."""
        if is_image(value):
            found.append([value["mime"], value["data"]])
            return f"[image {len(found)}]"
        if (self.files is not None and isinstance(value, Mapping) and isinstance(value.get("path"), str)
                and str(value.get("media", "")).startswith("image/")):
            root = Path(self.files).resolve()
            path = (root / value["path"]).resolve()
            if root in path.parents and path.is_file():
                found.append([value["media"], base64.b64encode(path.read_bytes()).decode("ascii")])
                return {**value, "shown": f"[image {len(found)}]"}
            return value
        if isinstance(value, (list, tuple)):
            return [self._pictures(item, found) for item in value]
        if isinstance(value, Mapping):
            return {key: self._pictures(item, found) for key, item in value.items()}
        return value

    def _closing(self, text: str) -> Any:
        if not self.json:
            return text
        text = text.strip()
        if text.startswith("```"):          # many models put their JSON in a fenced block
            text = text.split("\n", 1)[-1].rsplit("```", 1)[0]
        try:
            return json.loads(text)
        except ValueError as exc:
            raise NodeError(f"the model's answer is not JSON: {exc}") from None

    def _send(self, ctx, port: str, payload: Any) -> Any:
        if self.area is not None and port in self._paths:
            problem = self.area.sends(payload)
            if problem:
                raise Invalid(port, problem, again=True)
        return ctx.emit(port, payload)

    def _use(self, ctx, name: str, args: Mapping) -> str:
        """Answer one tool call; a failure becomes the answer, for the model to read."""
        try:
            if self._choose and name.startswith("send_") and name[5:] in self._sends:
                if "content" not in args:
                    return "error: content is missing"
                sent = self._send(ctx, name[5:], args["content"])
                if self.resident:
                    self.board["sent"][name[5:]] = self.board["sent"].get(name[5:], 0) + 1
                    if isinstance(sent, str):
                        return f"sent, as {sent}"
                return "sent"
            if name == "reject":
                port = str(args.get("port") or (next(iter(self.inputs)) if len(self.inputs) == 1 else ""))
                which = str(args["which"]) if args.get("which") else None
                if port not in self.inputs or not ctx.challengeable(port, which):
                    return f"error: what came on {port!r}{f' from {which}' if which else ''} cannot be rejected " \
                           f"(now, or any more)"
                return _verdict(ctx.reject(port, str(args.get("reason", "")), which))
            if name in self._own:
                return _text(self._own[name].run(dict(args), ctx))      # type: ignore[misc]
            if name not in self.outputs or not self.outputs[name].call:
                return f"error: there is no tool {name}"
            return _text(ctx.call(name, dict(args)))
        except Invalid as exc:     # what it sent did not pass a check: its to correct while the port allows
            if not exc.again:
                raise
            return f"not accepted: {exc.problem}. Put that right and send it again."
        except Exception as exc:   # noqa: BLE001 - whatever went wrong is the model's to read
            if ctx.stopping():
                raise
            return f"error: {exc}"

