"""Run one leased graph with the regular engine and a database-backed human host."""
from __future__ import annotations

import threading
import time
from dataclasses import asdict
from uuid import uuid4

from ..base import Ref, canonical
from ..engine import Engine
from ..protocols import Store, standard
from ..engine.result import WaitingForTasks
from ..runs import models_from_environment
from ..spec import Catalog, load, load_node
from .database import Database, Lease, LostLease, PostgresJournal, Refused, encoded
from .service import audit
from .validation import check_compiled_assignments


class RunStore(Store):
    """Artifact bytes shared by the workers of one project, checked by the regular Store."""
    def __init__(self, database, lease, project):
        self.database, self.lease, self.project = database, lease, project

    def _write(self, digest, data):
        ref = self.database.content.put(data)
        with self.database.fenced(self.lease) as db:
            db.execute("INSERT INTO il_files VALUES (%s,%s,%s) ON CONFLICT DO NOTHING", (self.project, ref.digest, ref.size))

    def _read(self, digest):
        with self.database.fenced(self.lease) as db:
            row = db.execute("SELECT size FROM il_files WHERE project=%s AND digest=%s",
                             (self.project, digest)).fetchone()
        return self.database.content.get(Ref(digest, row["size"])) if row else None


def _bind_task(db, lease, task, node, turn):
    """Record the human's own binding and each containing graph activation under the run lock."""
    parts = node.split("/")
    scope = []
    bindings = [part.rsplit(".", 1) for part in parts[:-1]]
    for name, round in [*bindings, (parts[-1], turn)]:
        key = encoded(scope)
        row = db.execute("SELECT record FROM il_events WHERE run=%s AND scope=%s AND record->>'kind'='bind' "
                         "AND record->>'node'=%s AND record->>'round'=%s ORDER BY seq DESC LIMIT 1",
                         (lease.run, key, name, str(round))).fetchone()
        if row is None:
            raise Refused("human task has no durable input binding")
        for delivery in row["record"]["took"]:
            db.execute("INSERT INTO il_task_reads VALUES (%s,%s,%s,%s)", (task, lease.run, key, delivery))
        scope.append([name, int(round)])
    return db.execute("SELECT 1 FROM il_task_reads JOIN il_revisions USING(run,scope,delivery) WHERE task=%s LIMIT 1",
                      (task,)).fetchone() is not None


def execute(database: Database, lease: Lease, *, seconds=30, allow=(), models=None):
    """Drive the command until it finishes or waits. Reclaim after a worker exits via Database.claim."""
    with database.fenced(lease) as db:
        run = db.execute("SELECT graph,inputs,project,assignments,workers,shared_workers,description IS NULL AS first_load "
                         "FROM il_runs WHERE id=%s", (lease.run,)).fetchone()

        authorization = db.execute("SELECT COALESCE(MAX(id),0) AS id FROM il_commands "
                                   "WHERE run=%s AND id<=%s AND key LIKE 'retry/%%'",
                                   (lease.run, lease.command)).fetchone()["id"]
    journal = PostgresJournal(database, lease, retry_authorization=authorization)

    def ask(node, turn, request, schema):
        with database.fenced(lease) as db:
            task = db.execute("SELECT * FROM il_tasks WHERE run=%s AND node=%s AND turn=%s",
                              (lease.run, node, turn)).fetchone()
            if task is None:
                task_id = str(uuid4())
                assignee = run["assignments"].get(node)
                db.execute("INSERT INTO il_tasks(id,run,node,turn,request,answer_schema,state,assignee) "
                           "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)", (task_id, lease.run, node, turn, encoded(request),
                            encoded(schema), "claimed" if assignee else "open", assignee))
                audit(db, run["project"], lease.worker, "task.open", task_id,
                      {"run": lease.run, "node": node, "turn": turn, "assignee": assignee})
                if _bind_task(db, lease, task_id, node, turn):
                    db.execute("UPDATE il_tasks SET state='superseded',version=version+1 WHERE id=%s", (task_id,))
                    PostgresJournal(database, lease).wake_superseded(db, task_id)
            else:
                task_id = str(task["id"])
                if canonical(task["request"]) != canonical(request) or task["answer_schema"] != schema:
                    raise Refused("replayed human request differs from its recorded input or answer schema")
                if task["state"] == "submitted":
                    return task["answer"]
        raise WaitingForTasks([task_id])

    done, lost = threading.Event(), threading.Event()

    engine = None

    def heartbeat():
        renew_at = time.monotonic() + seconds / 3
        while not done.wait(min(0.25, seconds / 3)):
            try:
                if time.monotonic() >= renew_at:
                    database.renew(lease, seconds)
                    renew_at = time.monotonic() + seconds / 3
                if engine is not None:
                    scopes = [(scope, tasks) for scope in engine.active_scopes() if (tasks := scope.waiting_tasks())]
                    if scopes:
                        parked = [task for _, tasks in scopes for task in tasks]
                        with database.connect() as db:
                            rows = db.execute("SELECT id::text AS task FROM il_tasks WHERE run=%s AND id=ANY(%s::uuid[]) "
                                              "AND state IN ('submitted','superseded')", (lease.run, parked)).fetchall()
                        ready = {row["task"] for row in rows}
                        for scope, tasks in scopes:
                            if not ready.isdisjoint(tasks):
                                scope.wake_tasks(ready)
            except (LostLease, database.driver.Error):
                lost.set()
                return

    thread = threading.Thread(target=heartbeat, daemon=True)
    thread.start()
    try:
        try:
            nested = run["workers"] if run["shared_workers"] else 0
            catalog = Catalog(allow=allow, models=models, ask_typed=ask, max_workers=nested,
                              default_workers=nested,
                              protocols=standard(RunStore(database, lease, run["project"])))
            graph = load(run["graph"], catalog)
            if run["first_load"]:
                check_compiled_assignments(graph, run["assignments"])
            with database.fenced(lease) as db:
                db.execute("UPDATE il_runs SET description=%s WHERE id=%s AND description IS NULL",
                           (encoded(graph.describe()), lease.run))
            engine = Engine(graph, catalog.protocols, workers=run["workers"], journal=journal,
                            stopping=lost.is_set, shared_workers=run["shared_workers"],
                            rebuild=lambda spec: load_node(spec, catalog))
            result = engine.go(run["inputs"])
            state, value = ("done" if result.complete else "stopped"), asdict(result)
        except WaitingForTasks as pending:
            state, value = "waiting", {"tasks": pending.tasks}
        except (LostLease, database.driver.OperationalError):
            raise
        except Exception as exc:
            state, value = "failed", {"error": str(exc)}
        if lost.is_set():
            raise LostLease("worker lost its lease during execution")
        database.finish(lease, state, value)
        return {"run": lease.run, "state": state}
    finally:
        done.set()
        thread.join()


def work_one(database, worker, *, seconds=30, allow=(), enable_models=False):
    lease = database.claim(worker, seconds)
    if lease is None:
        return None
    return execute(database, lease, seconds=seconds, allow=allow,
                   models=models_from_environment if enable_models else None)
