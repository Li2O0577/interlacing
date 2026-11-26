"""SQLite journals for one run and its nested activations."""
from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from contextlib import closing, contextmanager
from pathlib import Path

from .journal import JournalError


class SQLiteJournal:
    """Keep journals in one local SQLite file, with a separate sequence per scope."""

    def __init__(self, path: str | Path, *, durable: bool = False, checkpoint_every: int = 64,
                 scope: tuple[tuple[str, int], ...] = ()):
        if type(checkpoint_every) is not int or checkpoint_every < 1:
            raise ValueError("checkpoint_every must be a positive integer")
        self.path = Path(path)
        self.durable, self.checkpoint_every = durable, checkpoint_every
        self._scope = scope
        self._keys = tuple(json.dumps(scope[:depth], separators=(",", ":"))
                           for depth in range(len(scope) + 1))
        self._key = self._keys[-1]

    @contextmanager
    def _connection(self, *, write: bool = False) -> Iterator[sqlite3.Connection]:
        mode = "rwc" if write else "ro"
        try:
            with closing(sqlite3.connect(f"{self.path.resolve().as_uri()}?mode={mode}", uri=True,
                                         isolation_level=None)) as connection:
                yield connection
        except sqlite3.Error as error:
            raise JournalError(f"{self.path}: {error}") from error

    def _version(self, connection: sqlite3.Connection) -> int:
        version = int(connection.execute("PRAGMA user_version").fetchone()[0])
        if version not in (0, 1, 2):
            raise JournalError(f"{self.path}: unsupported SQLite journal version {version}")
        return version

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connection(write=True) as connection:
            if self._version(connection) == 0:
                mode = connection.execute("PRAGMA journal_mode = WAL").fetchone()[0]
                if mode != "wal":
                    raise JournalError(f"{self.path}: SQLite journal requires WAL mode")
            connection.execute("PRAGMA synchronous = FULL")
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("BEGIN IMMEDIATE")
            with connection:
                version = self._version(connection)
                if version == 0:
                    connection.execute(
                        "CREATE TABLE runs (scope TEXT PRIMARY KEY, parent TEXT REFERENCES runs(scope) ON DELETE CASCADE)"
                    )
                    connection.execute("CREATE INDEX runs_parent ON runs(parent)")
                    connection.execute(
                        "CREATE TABLE run_events (scope TEXT NOT NULL REFERENCES runs(scope) ON DELETE CASCADE, "
                        "seq INTEGER NOT NULL, record TEXT NOT NULL, PRIMARY KEY (scope, seq))"
                    )
                    connection.execute("PRAGMA user_version = 1")
                if version < 2 and self.durable:
                    connection.execute(
                        "CREATE TABLE checkpoints (scope TEXT PRIMARY KEY REFERENCES runs(scope) ON DELETE CASCADE, "
                        "seq INTEGER NOT NULL, state TEXT NOT NULL)"
                    )
                    connection.execute("PRAGMA user_version = 2")
                yield connection

    def inspect(self) -> Iterator[dict]:
        """Read an ordered SQLite snapshot without opening a write transaction."""
        if not self.path.exists():
            return
        with self._connection() as connection:
            if self._version(connection) == 0:
                return
            rows = connection.execute("SELECT seq, record FROM run_events WHERE scope = ? ORDER BY seq", (self._key,))
            for seq, record in rows:
                try:
                    yield json.loads(record)
                except ValueError as error:
                    raise JournalError(f"{self.path}: event {seq} in scope {self._key} is damaged: {error}") from error

    def read(self) -> list[dict]:
        return list(self.inspect())

    def has_records(self) -> bool:
        if not self.path.exists():
            return False
        with self._connection() as connection:
            if not self._version(connection):
                return False
            return connection.execute("SELECT 1 FROM run_events WHERE scope = ? LIMIT 1", (self._key,)).fetchone() is not None

    def recovery(self) -> tuple[dict | None, list[dict]]:
        """Read the latest recovery state and later events from one SQLite snapshot."""
        with self._connection() as connection:
            connection.execute("BEGIN")
            version = self._version(connection)
            if not version:
                return None, []
            row = None
            if version >= 2:
                row = connection.execute("SELECT seq, state FROM checkpoints WHERE scope = ?", (self._key,)).fetchone()
            seq, state = row if row is not None else (0, None)
            records = connection.execute("SELECT record FROM run_events WHERE scope = ? AND seq > ? ORDER BY seq",
                                         (self._key, seq))
            try:
                checkpoint = json.loads(state) if state is not None else None
                if checkpoint is not None and checkpoint.get("seq") != seq:
                    raise ValueError("checkpoint position does not match its state")
                return checkpoint, [json.loads(record) for record, in records]
            except (AttributeError, ValueError) as error:
                raise JournalError(f"{self.path}: invalid recovery data: {error}") from error

    def checkpoint(self, state: dict) -> None:
        payload = json.dumps(state, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
        with self._transaction() as connection:
            tail = connection.execute("SELECT MAX(seq) FROM run_events WHERE scope = ?", (self._key,)).fetchone()[0]
            if tail != state["seq"]:
                raise JournalError(f"{self.path}: journal advanced while saving its checkpoint")
            connection.execute("INSERT INTO checkpoints (scope, seq, state) VALUES (?, ?, ?) "
                               "ON CONFLICT(scope) DO UPDATE SET seq = excluded.seq, state = excluded.state",
                               (self._key, tail, payload))

    def write(self, record: dict) -> None:
        payload = json.dumps(record, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
        with self._transaction() as connection:
            parent = None
            for key in self._keys:
                connection.execute("INSERT OR IGNORE INTO runs (scope, parent) VALUES (?, ?)", (key, parent))
                parent = key
            connection.execute("INSERT INTO run_events (scope, seq, record) "
                               "SELECT ?, COALESCE(MAX(seq), 0) + 1, ? FROM run_events WHERE scope = ?",
                               (self._key, payload, self._key))

    def close(self) -> None:
        """Connections belong to individual operations and are closed there."""

    def reset(self) -> None:
        if self.path.exists():
            with self._transaction() as connection:
                if self._version(connection) >= 2:
                    connection.execute("DELETE FROM checkpoints WHERE scope = ?", (self._key,))
                connection.execute("DELETE FROM run_events WHERE scope = ?", (self._key,))

    def inner(self, node: str, round: int) -> SQLiteJournal:
        return SQLiteJournal(self.path, durable=self.durable, checkpoint_every=self.checkpoint_every,
                             scope=(*self._scope, (node, round)))

    def discard(self) -> None:
        if self.path.exists():
            with self._transaction() as connection:
                connection.execute("DELETE FROM runs WHERE scope = ?", (self._key,))

    def prune_empty_inner(self) -> None:
        """Child scopes are created on write and removed by discard."""
