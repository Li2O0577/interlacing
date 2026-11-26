"""What a run gives back: its result, or the error it ended with."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ..base import NodeError


class WaitingForTasks(BaseException):
    """Durable activations waiting for external answers, including inside nested graphs."""

    def __init__(self, tasks: list[str]):
        super().__init__(tasks)
        self.tasks = tasks


class OverBudget(NodeError):
    """Raised in a node when what it reports using takes the run over its budget. The run has failed by
    then; the node should return."""


class RunError(RuntimeError):
    """A node failed, or the run was given something the graph does not take. ``node`` names the node."""

    def __init__(self, message: str, node: str | None = None):
        super().__init__(message)
        self.node = node
        self.problems: list[dict] = []     # what had failed a check before the run failed
        self.used: dict[str, float] = {}   # what the run had used up when it failed


@dataclass
class Result:
    outputs: dict[str, list] = field(default_factory=dict)   # everything that left each graph output, in order
    activations: int = 0
    envelopes: int = 0
    rounds: dict[str, int] = field(default_factory=dict)     # how many times each node ran
    limited: tuple[str, ...] = ()                            # back edges that reached max_rounds and dropped an envelope
    complete: bool = True                                    # False when the run was stopped with work left
    problems: list[dict] = field(default_factory=list)       # what failed a check and was repaired or let through
    used: dict[str, float] = field(default_factory=dict)     # what the nodes reported using, added up
    held: dict[str, str] = field(default_factory=dict)       # nodes whose switch was not on at the end: paused or stopped

    def last(self, name: str, default: Any = None) -> Any:
        values = self.outputs.get(name)
        return values[-1] if values else default
