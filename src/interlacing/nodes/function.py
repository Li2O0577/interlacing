"""A node that is one Python function, and the short ways of making one.

The long way names everything: ``FunctionNode("double", f, inputs=[Port("x", int)], outputs=[Port("y", int)])``.
The short way reads it all from the function: ``as_node(f)``, or ``@node`` over its definition, and every
method takes a plain function wherever it takes a node.
"""
from __future__ import annotations

import inspect
import time
import typing
from typing import Any, Callable, Iterable, Mapping

from ..base import Firing, Method, Node, NodeError, Port, PortError, SchemaError, schema


def named(function: Callable) -> str:
    """A function's dotted name, for a node's ``config``."""
    return f"{getattr(function, '__module__', '')}.{getattr(function, '__qualname__', type(function).__qualname__)}"


class FunctionNode(Node):
    """Calls ``function`` with the arrived inputs as keyword arguments, by port name.

    With one output port (not counting call ports), what the function returns is that port's payload (``None`` sends nothing). With
    several, it returns a mapping of port name to payload; ports it leaves out send nothing. With
    ``context=True`` the function also receives the engine's context as its first argument, to emit several
    times or to see that the run is stopping.

    The function may be ``async def``: it is run to its end in an event loop of its own, in the thread of the
    activation, so it can await what it likes without holding up any other node.
    """

    kind = "function"

    def __init__(self, id: str, function: Callable, *, inputs: Iterable[Port] = (), outputs: Iterable[Port] = (),
                 firing: Firing | None = None, context: bool = False, retry_attempts: int = 1,
                 retry_on=("connection", "timeout")):
        if isinstance(function, FunctionNode):
            function = function.function   # a function made into a node with @node: the function itself
        if not callable(function):
            raise NodeError(f"{id}: not callable: {function!r}")
        super().__init__(id, inputs=inputs, outputs=outputs, firing=firing)
        self.function: Callable = function
        self.context = context
        if type(retry_attempts) is not int or not 1 <= retry_attempts <= 16:
            raise NodeError(f"{id}: retry_attempts must be an integer from 1 to 16")
        if retry_attempts > 1 and (context or self.asynchronous):
            raise NodeError(f"{id}: retry_attempts requires a synchronous function without context")
        if not isinstance(retry_on, (list, tuple)) or not retry_on or any(
                name not in ("connection", "timeout") for name in retry_on):
            raise NodeError(f"{id}: retry_on must list connection or timeout")
        self.retry_on = tuple(sorted(set(retry_on)))
        self.retry_attempts = retry_attempts
        self._sends = [name for name, port in self.outputs.items() if not port.call]

    def config(self) -> Mapping:
        return {"function": named(self.function), "context": self.context,
                **({"retry_on": list(self.retry_on)} if self.retry_on != ("connection", "timeout") else {}),
                **({"retry_attempts": self.retry_attempts} if self.retry_attempts > 1 else {})}

    @property
    def asynchronous(self) -> bool:
        function = self.function
        return inspect.iscoroutinefunction(function) or inspect.iscoroutinefunction(getattr(function, "__call__", None))

    def run(self, ctx, inputs: Mapping[str, Any]):
        if self.retry_attempts > 1:
            return self._retry(ctx, inputs)
        result = self.function(ctx, **inputs) if self.context else self.function(**inputs)
        if inspect.isawaitable(result):
            return _Later(self, result)          # the engine awaits it
        return self._shaped(result)

    def _retry(self, ctx, inputs):
        authorization = getattr(ctx, "retry_authorization", 0)
        budget = 0
        while True:
            prefix = f"function-retry-{budget}-attempt" if budget else "function-attempt"
            for attempt in range(1, self.retry_attempts + 1):
                outcome = ctx.once(f"{prefix}-{attempt}", lambda: self._attempt(inputs, attempt, authorization))
                if "value" in outcome:
                    return self._shaped(outcome["value"])
                if attempt < self.retry_attempts:
                    while (remaining := outcome["next"] - time.time()) > 0:
                        if ctx.stopping():
                            return None
                        time.sleep(min(remaining, 0.05))
                    if ctx.stopping():
                        return None
            if authorization <= outcome.get("authorization", 0):
                error = TimeoutError if outcome["timeout"] else ConnectionError
                raise error(f"{self.id} exhausted {self.retry_attempts} attempts: {outcome['error']}")
            # Follow committed budgets in order, even if another command took over later.
            budget = ctx.once(f"function-retry-after-{budget}", lambda: authorization)

    def _attempt(self, inputs, attempt, authorization):
        try:
            return {"value": self.function(**inputs)}
        except (ConnectionError, TimeoutError) as exc:
            timeout = isinstance(exc, TimeoutError)
            if ("timeout" if timeout else "connection") not in self.retry_on:
                raise
            delay = min(5.0, 0.1 * 2 ** (attempt - 1))
            return {"error": str(exc), "timeout": timeout, "next": time.time() + delay,
                    "authorization": authorization}

    def _shaped(self, result) -> Mapping[str, Any] | None:
        if result is None:
            return None
        if len(self._sends) == 1:
            return {self._sends[0]: result}
        if not isinstance(result, Mapping):
            raise NodeError(f"{self.id} has {len(self._sends)} output ports: its function must return a mapping of port to payload")
        return result

    def __call__(self, *args, **kwargs):
        """The function itself, so that a function made into a node with ``@node`` can still be called."""
        return self.function(*args, **kwargs)

    @classmethod
    def of(cls, function: Callable, id: str | None = None, *, outputs: Iterable[Port | str] | None = None,
           firing: Firing | None = None, check: Any = (), repairs: int | None = None, invalid: str = "fail") -> "FunctionNode":
        """A node made from what ``function`` says about itself.

        * its id is the function's name;
        * each parameter is an input of the same name; one with a default value is optional; a first
          parameter called ``ctx`` is the engine's context and not an input;
        * a parameter's annotation is the port's schema, where it is one (``int``, ``str``, ``list[float]``,
          ``dict``...); anything else is taken as any value;
        * the output is ``out``, with the return annotation as its schema; ``-> None`` means no output.
          ``outputs`` names others instead (the function then returns a mapping of port to payload);
        * ``check``, ``repairs`` and ``invalid`` go to the output port when there is one.
        """
        if isinstance(function, FunctionNode):
            function = function.function
        if not callable(function):
            raise NodeError(f"not a function: {function!r}")
        name: str = id or getattr(function, "__name__", "") or ""
        if not id and (not name or name == "<lambda>"):
            raise NodeError("a function with no name of its own (a lambda) needs an id: as_node(f, 'name')")
        try:
            signature = inspect.signature(function)
        except (TypeError, ValueError) as exc:
            raise NodeError(f"{name}: its parameters cannot be read: {exc}") from None
        hints = _hints(function)
        params = list(signature.parameters.values())
        context = bool(params) and params[0].name == "ctx"
        if context:
            params = params[1:]
        inputs = []
        for param in params:
            if param.kind in (param.VAR_POSITIONAL, param.VAR_KEYWORD):
                raise NodeError(f"{name}: *{param.name} cannot be an input; name each input as a parameter")
            if param.kind is param.POSITIONAL_ONLY:
                raise NodeError(f"{name}: {param.name} is positional only; inputs are passed by name")
            try:
                inputs.append(Port(param.name, _schema(hints.get(param.name)),
                                   required=param.default is inspect.Parameter.empty))
            except PortError as exc:
                raise NodeError(f"{name}: {exc}") from None
        rule = {"check": check, "repairs": repairs, "invalid": invalid}
        try:
            if outputs is not None:
                made = [out if isinstance(out, Port) else Port(out, **rule) for out in outputs]
            elif "return" in hints and hints["return"] is type(None):
                made = []
            else:
                made = [Port("out", _schema(hints.get("return")), **rule)]
        except PortError as exc:
            raise NodeError(f"{name}: {exc}") from None
        return cls(name, function, inputs=inputs, outputs=made, firing=firing, context=context)


def _hints(function: Callable) -> dict:
    """The function's annotations, resolved where they can be."""
    try:
        return typing.get_type_hints(function)
    except Exception:   # noqa: BLE001 - an annotation that names something not to be found: take it as given
        return dict(getattr(function, "__annotations__", {}) or {})


def _schema(hint: Any):
    """The schema an annotation stands for, or anything when it stands for none."""
    if hint is None or hint is Any or isinstance(hint, str):
        return None
    origin, args = typing.get_origin(hint), typing.get_args(hint)
    if origin in (list, tuple, set, frozenset, dict) and origin is not None:
        if origin is list and len(args) == 1:
            inner = _schema(args[0])
            return [inner] if inner is not None else list
        return origin
    if origin is not None:      # a union, a literal and so on: not checked by shape
        return None
    if hint is float:
        return (float, int)     # a whole number is a number too
    try:
        return schema(hint)
    except SchemaError:
        return None


def as_node(thing: Any, id: str | None = None) -> Node:
    """``thing`` as a node: a node as it is, a plain function made into one (``FunctionNode.of``), and a
    method (a chain, a fan...) as a ``GraphNode`` called after the method unless an id is given."""
    if isinstance(thing, Node):
        return thing
    if isinstance(thing, Method):
        from .graph import GraphNode
        return GraphNode(id or thing.name or "graph", thing)
    if callable(thing):
        return FunctionNode.of(thing, id)
    raise NodeError(f"not a node or a function: {thing!r}")


def as_nodes(things: Iterable[Any]) -> tuple[Node, ...]:
    """Each of ``things`` as a node (see ``as_node``). A plain function (or a method) that comes more than once
    becomes a node each time, the second called ``<name>_2``, the third ``<name>_3`` and so on."""
    made: list[Node] = []
    seen: dict[str, int] = {}
    for thing in things:
        if isinstance(thing, Node) or not (callable(thing) or isinstance(thing, Method)):
            made.append(as_node(thing))
            continue
        name = (thing.name or "graph") if isinstance(thing, Method) else getattr(thing, "__name__", "")
        seen[name] = count = seen.get(name, 0) + 1
        made.append(as_node(thing, f"{name}_{count}" if count > 1 and name != "<lambda>" else None))
    return tuple(made)


def node(function: Callable | None = None, /, *, id: str | None = None, outputs: Iterable[Port | str] | None = None,
         firing: Firing | None = None, check: Any = (), repairs: int | None = None, invalid: str = "fail"):
    """Make a function into a node where it is defined, writing only what differs from what the function
    says about itself (see ``FunctionNode.of``). The node can still be called as the function.

        @node
        def double(x: int) -> int: ...

        @node(outputs=["kept", "dropped"], firing=Firing.ANY)
        def sort(x): ...
    """
    def make(function: Callable) -> FunctionNode:
        return FunctionNode.of(function, id, outputs=outputs, firing=firing, check=check, repairs=repairs, invalid=invalid)
    return make(function) if function is not None else make


class _Later:
    """What an ``async def`` function gave, to be awaited: then shaped like any function's answer."""

    __slots__ = ("node", "pending")

    def __init__(self, node: FunctionNode, pending):
        self.node, self.pending = node, pending

    def __await__(self):
        return self.node._shaped((yield from self.pending.__await__()))

    def close(self) -> None:
        close = getattr(self.pending, "close", None)
        if close is not None:
            close()
