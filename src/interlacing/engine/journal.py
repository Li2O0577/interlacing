"""The journal: a file to which a run appends what has happened, so that it can be taken up again.

One line of JSON per record:

* ``run``: the graph's digest and the envelopes made from the graph's inputs;
* ``act``: one finished activation, written in one piece when it ends: which envelopes it took, which it
  sent, what reached the graph's outputs, the node's own snapshot;
* ``change``: the run continues under another graph;
* ``end``: nothing was left to do.

An activation that did not finish leaves no record: after a restart its inputs are still waiting and it runs
again. A line cut short by the process dying is dropped when the journal is next read.
"""
from __future__ import annotations

import json
import os
import shutil
from collections.abc import Iterator
from pathlib import Path
from typing import Any, Protocol

from ..base import Envelope, Ref
from .stream import Stream


class JournalError(ValueError):
    pass


class JournalBackend(Protocol):
    """Storage for one run, with a separate journal per nested activation."""

    def read(self) -> list[dict]:
        """Read the recoverable records in order."""
        ...

    def write(self, record: dict) -> None:
        """Append one record."""
        ...

    def close(self) -> None:
        """Release this handle without removing its records."""
        ...

    def reset(self) -> None:
        """Clear this run's records so it can start afresh."""
        ...

    def inner(self, node: str, round: int) -> JournalBackend:
        """Open the same child journal for a given node and activation round."""
        ...

    def discard(self) -> None:
        """Remove this journal and the journals of all its descendants."""
        ...

    def prune_empty_inner(self) -> None:
        """Release unused child storage when this run ends."""
        ...


class Journal:
    """``sync=True`` also asks the operating system to put every record on the disk before going on, which
    survives a power cut and costs a disk write per activation."""

    def __init__(self, path, *, sync: bool = False):
        self.path, self.sync = Path(path), sync
        self._file: Any = None

    def read(self) -> list[dict]:
        """Every complete record, oldest first."""
        try:
            data = self.path.read_bytes()
        except FileNotFoundError:
            return []
        records, good = [], 0
        for number, line in enumerate(data.splitlines(keepends=True), 1):
            try:
                if not line.endswith(b"\n"):
                    raise ValueError("cut short")
                records.append(json.loads(line))
            except ValueError as exc:
                if good + len(line) != len(data):
                    raise JournalError(f"{self.path}: line {number} is damaged: {exc}") from None
                self.close()
                with open(self.path, "r+b") as file:      # the last line was being written when the process died
                    file.truncate(good)
                break
            good += len(line)
        return records

    def inspect(self) -> Iterator[dict]:
        """Read complete records up to the size observed at open, without repairing the journal."""
        try:
            file = self.path.open("rb")
        except FileNotFoundError:
            return
        with file:
            remaining = os.fstat(file.fileno()).st_size
            number = 0
            while remaining:
                line = file.readline(remaining)
                if not line.endswith(b"\n"):
                    return
                remaining -= len(line)
                number += 1
                try:
                    yield json.loads(line)
                except ValueError as error:
                    raise JournalError(f"{self.path}: line {number} is damaged: {error}") from None

    def write(self, record: dict) -> None:
        line = json.dumps(record, ensure_ascii=False, separators=(",", ":"), allow_nan=False) + "\n"
        if self._file is None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._file = open(self.path, "ab")
        self._file.write(line.encode("utf-8"))
        self._file.flush()
        if self.sync:
            os.fsync(self._file.fileno())

    def close(self) -> None:
        if self._file is not None:
            self._file.close()
            self._file = None

    def reset(self) -> None:
        self.close()
        self.path.unlink()

    @property
    def _inner_dir(self) -> Path:
        return self.path.with_name(self.path.name + ".d")

    def inner(self, node: str, round: int) -> Journal:
        return Journal(self._inner_dir / f"{node}-{round}.jsonl", sync=self.sync)

    def discard(self) -> None:
        self.close()
        shutil.rmtree(self._inner_dir, ignore_errors=True)
        self.path.unlink(missing_ok=True)

    def prune_empty_inner(self) -> None:
        try:
            self._inner_dir.rmdir()
        except OSError:
            pass  # never created, or still holds journals


def pack_body(body: Any) -> list:
    if isinstance(body, Ref):
        return ["r", body.digest, body.size, body.media]
    if isinstance(body, Stream):
        return ["s", [pack_body(chunk) for chunk in body._kept or ()]]
    return ["v", body]


def unpack_body(packed: list, protocol=None, port=None) -> Any:
    kind = packed[0]
    if kind == "r":
        return Ref(packed[1], packed[2], packed[3])
    if kind == "s":
        stream = Stream(protocol, port)
        for chunk in packed[1]:
            stream._put(unpack_body(chunk))
        stream._close()
        return stream
    return packed[1]


def pack(envelope: Envelope) -> list:
    packed = [envelope.edge, envelope.seq, envelope.round, pack_body(envelope.body), list(envelope.cause)]
    return [*packed, envelope.replaces] if envelope.replaces else packed


def history_record(record: dict, seq: int) -> dict:
    """Keep provenance references without message bodies, snapshots or node definitions."""
    kind = record["kind"]
    event = {"seq": seq, "kind": kind, "schema_version": record.get("schema_version", 0)}
    if kind == "run":
        event.update(run_id=record.get("id"), scope_id=record.get("scope_id"))
    if kind in ("run", "fork", "act", "deliver", "correct"):
        event["deliveries"] = [
            [f"{item[0]}#{item[1]}", item[5] if len(item) > 5 else None]
            for item in record.get("sent", ())
        ]
    if kind == "act":
        event.update(node=record["node"], round=record.get("round"), reads=record.get("took"),
                     emits=record.get("emits", []), outputs=[name for name, _ in record.get("out", ())],
                     output_indices=record.get("out_indices"),
                     revised_outputs=[[name, index] for name, index, _ in record.get("fixed_out", ())])
        if record.get("child_history"):
            event["child_history"] = record["child_history"]
        if record.get("stale"):
            event["stale"] = True
    if kind in ("deliver", "correct"):
        outputs = record["outputs"]
        if kind == "deliver":
            event["outputs"] = [name for name, _, _ in outputs]
            event["output_indices"] = [index for _, index, _ in outputs]
            event["message_version"] = 1
        else:
            event["revised_outputs"] = [[name, index] for name, index, _ in outputs]
            event["message_version"] = record["version"]
        event["emits"] = [[record["token"], record["port"],
                           [[packed[0], delivery] for packed, (delivery, _) in zip(record["sent"], event["deliveries"])],
                           [[name, index] for name, index, _ in outputs]]]
    return event
