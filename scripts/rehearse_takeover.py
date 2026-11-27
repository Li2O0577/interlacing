"""Crash and recover one committed binding in a dedicated empty PostgreSQL test database."""
import argparse
import base64
import json
import os
import socket
from time import monotonic, sleep
from uuid import UUID

from interlacing.base import Ref
from interlacing.collab.database import Database, PostgresJournal
from interlacing.collab.service import Collaboration
from interlacing.collab.worker import execute

PROJECT = "interlacing-host-handoff"
PAYLOAD = b"interlacing host handoff"


def crash(database):
    database.install()
    with database.connect() as db:
        if db.execute("SELECT 1 FROM il_projects LIMIT 1").fetchone():
            raise ValueError("use an empty rehearsal database with no other workers")
    service = Collaboration(database)
    token = service.bootstrap(PROJECT, "operator")["token"]
    resource = service.request(token, "put", {"namespace": "shared", "name": "receipt",
                                             "file": base64.b64encode(PAYLOAD).decode()})
    run = service.request(token, "start", {"graph": {"method": "chain", "nodes": [{"id": "echo", "kind": "pass"}]},
                                         "resources": {"in": resource}})["run"]
    lease = database.claim(socket.gethostname(), 3)
    if lease is None or lease.run != run:
        raise RuntimeError("unexpected claim")
    print(json.dumps({"run": run, "primary": lease.worker, "generation": lease.generation}), flush=True)
    write = PostgresJournal.write

    def crash_after_bind(self, record):
        write(self, record)
        if record["kind"] == "bind":
            os._exit(43)

    PostgresJournal.write = crash_after_bind
    execute(database, lease, seconds=3)
    raise RuntimeError("fault injection did not fire")


def recover(database, run):
    with database.connect() as db:
        row = db.execute("SELECT worker,generation FROM il_runs WHERE id=%s AND project=%s AND state='running'",
                         (run, PROJECT)).fetchone()
        if not row or row["generation"] != 1:
            raise ValueError("expected the interrupted run from the crash step")
    start = monotonic()
    while monotonic() - start < 15:
        lease = database.claim(socket.gethostname(), 30)
        if lease:
            break
        sleep(.1)
    else:
        raise TimeoutError("lease did not expire")
    if lease.run != str(run) or lease.generation != 2 or execute(database, lease)["state"] != "done":
        raise RuntimeError("replacement failed to complete the interrupted run")
    with database.connect() as db:
        events = db.execute("SELECT record->>'kind' AS kind FROM il_events WHERE run=%s", (run,))
        kinds = [e["kind"] for e in events]
        result = db.execute("SELECT result FROM il_runs WHERE id=%s", (run,)).fetchone()["result"]
    output, = result["outputs"]["out"]
    if kinds.count("bind") != 1 or kinds.count("act") != 1:
        raise RuntimeError(f"unexpected execution history: {kinds}")
    if database.content.get(Ref(output["file"], output["size"])) != PAYLOAD:
        raise RuntimeError("shared content verification failed")
    print(json.dumps({"run": str(run), "primary": row["worker"], "replacement": lease.worker,
                      "generation": lease.generation, "seconds_from_recover": round(monotonic()-start, 3)}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    sub.add_parser("crash")
    sub.add_parser("recover").add_argument("run", type=UUID)
    args = parser.parse_args()
    db = Database(os.environ["INTERLACING_POSTGRES_DSN"])
    if args.action == "crash":
        crash(db)
    else:
        recover(db, args.run)
