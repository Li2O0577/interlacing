"""PostgreSQL transactions and fenced execution journals for the collaboration service."""
from __future__ import annotations

import json
import os
from contextlib import contextmanager
from dataclasses import dataclass
from importlib import import_module
from importlib.resources import files

from ..engine.journal import JournalError
from ..protocols import DirectoryStore


class Refused(ValueError):
    """The requested operation conflicts with the actor's permissions or the stored state."""


class RequestConflict(Refused):
    """The caller reused a start key with different arguments."""


class LostLease(Refused):
    """Another generation owns the run, or this lease has expired."""


def encoded(value):
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"))


@dataclass(frozen=True)
class Lease:
    run: str
    command: int
    generation: int
    worker: str


class Database:
    def __init__(self, dsn: str):
        try:
            self.driver = import_module("psycopg")
        except ImportError:
            raise Refused("install interlacing[postgres] to use the PostgreSQL service") from None
        self.dsn = dsn
        directory = os.getenv("INTERLACING_CONTENT_DIR")
        self._content = DirectoryStore(directory) if directory else None

    @property
    def content(self) -> DirectoryStore:
        if self._content is None:
            raise Refused("file and artifact storage requires a shared INTERLACING_CONTENT_DIR")
        return self._content

    def connect(self):
        return self.driver.connect(self.dsn, row_factory=self.driver.rows.dict_row)

    def install(self):
        with self.connect() as db:
            db.execute("SELECT pg_advisory_xact_lock(617244142)")
            db.execute("CREATE TABLE IF NOT EXISTS il_schema (singleton boolean PRIMARY KEY CHECK(singleton), "
                       "version integer NOT NULL)")
            db.execute("INSERT INTO il_schema VALUES (true,1) ON CONFLICT DO NOTHING")
            if db.execute("SELECT version FROM il_schema WHERE singleton").fetchone()["version"] not in (1, 2):
                raise Refused("unsupported collaboration database version")
            db.execute(files(__package__).joinpath("schema.sql").read_text(encoding="utf-8"))
            db.execute("UPDATE il_schema SET version=2 WHERE singleton AND version=1")

    @contextmanager
    def fenced(self, lease: Lease):
        with self.connect() as db:
            row = db.execute("SELECT id FROM il_runs WHERE id=%s AND worker=%s AND generation=%s "
                             "AND expires>clock_timestamp() FOR UPDATE",
                             (lease.run, lease.worker, lease.generation)).fetchone()
            if row is None:
                raise LostLease("run lease has expired or changed owner")
            yield db

    def claim(self, worker: str, seconds: float) -> Lease | None:
        if not worker or not 0 < seconds <= 86400:
            raise Refused("provide a worker name and a lease duration up to 86400 seconds")
        with self.connect() as db:
            row = db.execute(
                "SELECT r.id, c.id AS command FROM il_runs r JOIN LATERAL "
                "(SELECT id FROM il_commands WHERE run=r.id AND NOT done ORDER BY id LIMIT 1) c ON true "
                "WHERE r.expires IS NULL OR r.expires<=clock_timestamp() "
                "ORDER BY c.id LIMIT 1 FOR UPDATE OF r SKIP LOCKED").fetchone()
            if row is None:
                return None
            # Re-read after acquiring the run lock. The selected command may have finished
            # between the candidate scan and locking the updated run row.
            command = db.execute("SELECT id FROM il_commands WHERE run=%s AND NOT done ORDER BY id LIMIT 1",
                                 (row["id"],)).fetchone()
            if command is None:
                return None
            generation = db.execute("UPDATE il_runs SET worker=%s, expires=clock_timestamp()+%s*interval '1 second', "
                                    "generation=generation+1,state='running' WHERE id=%s RETURNING generation",
                                    (worker, seconds, row["id"])).fetchone()["generation"]
            return Lease(str(row["id"]), command["id"], generation, worker)

    def renew(self, lease, seconds):
        with self.fenced(lease) as db:
            db.execute("UPDATE il_runs SET expires=clock_timestamp()+%s*interval '1 second' WHERE id=%s",
                       (seconds, lease.run))

    def finish(self, lease, state, result):
        with self.fenced(lease) as db:
            db.execute("UPDATE il_commands SET done=true WHERE id=%s AND run=%s", (lease.command, lease.run))
            if state in ("done", "failed"):
                db.execute("UPDATE il_commands SET done=true WHERE run=%s AND NOT done", (lease.run,))
            elif db.execute("SELECT 1 FROM il_commands WHERE run=%s AND NOT done LIMIT 1", (lease.run,)).fetchone():
                state = "queued"
            db.execute("UPDATE il_runs SET state=%s, result=%s, worker=NULL, expires=NULL WHERE id=%s",
                       (state, encoded(result), lease.run))


class PostgresJournal:
    durable = True
    checkpoint_every = 64

    def __init__(self, database: Database, lease: Lease, scope=(), *, retry_authorization=0):
        self.retry_authorization = retry_authorization
        self.database, self.lease, self.scope = database, lease, scope
        self.key = (lease.run, encoded(scope))

    def inner(self, node, round):
        return type(self)(self.database, self.lease, (*self.scope, (node, round)),
                          retry_authorization=self.retry_authorization)

    def has_records(self):
        with self.database.connect() as db:
            return bool(db.execute("SELECT 1 FROM il_events WHERE run=%s AND scope=%s LIMIT 1", self.key).fetchone())

    def read(self):
        with self.database.connect() as db:
            return [r["record"] for r in db.execute("SELECT record FROM il_events WHERE run=%s AND scope=%s ORDER BY seq",
                                                    self.key)]

    def write(self, record):
        with self.database.fenced(self.lease) as db:
            db.execute("INSERT INTO il_events(run,scope,seq,record) SELECT %s,%s,COALESCE(MAX(seq),0)+1,%s "
                       "FROM il_events WHERE run=%s AND scope=%s", (*self.key, encoded(record), *self.key))

            if record.get("kind") == "correct":
                replaced = [item[5] for item, route in zip(record["sent"], record["routes"]) if route == "rerun"]
                for delivery in replaced:
                    db.execute("INSERT INTO il_revisions VALUES (%s,%s,%s) ON CONFLICT DO NOTHING", (*self.key, delivery))
                    tasks = db.execute("UPDATE il_tasks t SET state='superseded', version=version+1 "
                                       "FROM il_task_reads b WHERE b.task=t.id AND b.run=%s AND b.scope=%s "
                                       "AND b.delivery=%s AND t.state IN ('open','claimed') RETURNING t.id",
                                       (*self.key, delivery)).fetchall()
                    for task in tasks:
                        self.wake_superseded(db, task["id"])

    def wake_superseded(self, db, task):
        db.execute("INSERT INTO il_commands(run,key) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                   (self.lease.run, "superseded/" + str(task)))
        db.execute("INSERT INTO il_audit(project,actor,action,subject,details) "
                   "SELECT project,%s,'task.superseded',%s,'{}' FROM il_runs WHERE id=%s",
                   (self.lease.worker, str(task), self.lease.run))

    def recovery(self, *, at=None, required=None):
        with self.database.connect() as db:
            db.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            tail = db.execute("SELECT COALESCE(MAX(seq),0) AS seq FROM il_events WHERE run=%s AND scope=%s",
                              self.key).fetchone()["seq"]
            if at is not None and (type(at) is not int or not 1 <= at <= tail):
                raise JournalError("event position outside run history")
            through = tail if at is None else at
            row = db.execute("SELECT seq,state FROM il_checkpoints WHERE run=%s AND scope=%s AND seq<=%s "
                             "ORDER BY seq DESC LIMIT 1", (*self.key, through)).fetchone()
            state = row["state"] if row and (required is None or required in row["state"]) else None
            if state is not None and state["seq"] != row["seq"]:
                raise JournalError("checkpoint position does not match its state")
            start = state["seq"] if state else 0
            records = db.execute("SELECT record FROM il_events WHERE run=%s AND scope=%s AND seq>%s AND seq<=%s "
                                 "ORDER BY seq", (*self.key, start, through))
            return state, [item["record"] for item in records]

    def checkpoint(self, state):
        with self.database.fenced(self.lease) as db:
            seq = db.execute("SELECT MAX(seq) AS seq FROM il_events WHERE run=%s AND scope=%s", self.key).fetchone()["seq"]
            if seq != state["seq"]:
                raise JournalError("journal advanced while saving its checkpoint")
            db.execute("INSERT INTO il_checkpoints VALUES (%s,%s,%s,%s) ON CONFLICT (run,scope,seq) "
                       "DO UPDATE SET state=excluded.state", (*self.key, seq, encoded(state)))
            db.execute("DELETE FROM il_checkpoints WHERE run=%s AND scope=%s AND seq IN ("
                       "SELECT seq FROM il_checkpoints WHERE run=%s AND scope=%s ORDER BY seq DESC OFFSET 16)",
                       (*self.key, *self.key))

    def reset(self):
        with self.database.fenced(self.lease) as db:
            for table in ("il_checkpoints", "il_events", "il_revisions", "il_task_reads"):
                db.execute(f"DELETE FROM {table} WHERE run=%s AND scope=%s", self.key)

    def discard(self):
        with self.database.fenced(self.lease) as db:
            # Match each leading child activation in order, including duplicate names at different depths.
            for table in ("il_checkpoints", "il_events", "il_revisions", "il_task_reads"):
                predicates = ["run=%s", "jsonb_array_length(scope)>=%s"]
                args = [self.lease.run, len(self.scope)]
                for depth, part in enumerate(self.scope):
                    predicates.append("scope->%s::int=%s::jsonb")
                    args.extend([depth, encoded(part)])
                db.execute(f"DELETE FROM {table} WHERE " + " AND ".join(predicates), args)

    def close(self):
        pass

    def prune_empty_inner(self):
        pass
