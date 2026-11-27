"""Run a disposable single-host capacity and live-backup rehearsal. See docs/DEPLOY.md."""
from ast import literal_eval
import base64
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from contextlib import contextmanager
import json
import math
import os
from pathlib import Path
import shutil
import subprocess
import sys
from tempfile import TemporaryDirectory
import threading
from time import monotonic
from urllib.request import urlopen
from uuid import uuid4

from interlacing.collab.client import Client
from interlacing.collab.database import Database
from interlacing.collab.service import Collaboration
from interlacing.collab.worker import execute, work_one

from backup_collab import backup


@contextmanager
def scratch_database():
    name = "il_rehearse_" + uuid4().hex
    subprocess.run(["createdb", name], check=True)
    try:
        yield name
    finally:
        subprocess.run(["dropdb", name], check=True)


def take_command(_):
    db = Database("")
    lease = db.claim(f"bench/{os.getpid()}", 60)
    claimed = monotonic()
    result = execute(db, lease, seconds=60)
    assert result["state"] == "done", result
    return result["run"], claimed, monotonic()


def capacity(request, processes, size=32):
    graph = {"method": "chain", "nodes": [{"id": "passage", "kind": "pass"}]}
    started = {}
    begin = monotonic()
    for number in range(size):
        submitted = monotonic()
        run = request("start", graph=graph, inputs={"in": number})["run"]
        started[run] = (submitted, number)
    with ProcessPoolExecutor(processes) as pool:
        results = list(pool.map(take_command, range(size)))
    waits = []
    for run, claimed, done in results:
        submitted, number = started.pop(run)
        assert request("status", run=run)["result"]["outputs"] == {"out": [number]}
        waits.append(claimed - submitted)
    assert not started
    first = min(done for _, _, done in results)
    last = max(done for _, _, done in results)
    print(json.dumps(dict(processes=processes, runs=size, first_result_ms=round(1000*(first-begin), 2),
                          submit_to_claim_p95_ms=round(1000*sorted(waits)[math.ceil(size*.95)-1], 2),
                          runs_per_second=round(size/(last-begin), 2))))


def history_cost(request, db):
    for length in (1, 16, 64, 512):
        graph = {"method": "chain", "nodes": [{"id": f"step{i}", "kind": "pass"} for i in range(length)]}
        parent = request("start", graph=graph, inputs={"in": "a"*256})
        assert work_one(db, "history")["state"] == "done"
        seq = request("checkpoints", **parent)["last_event"]
        samples = []
        for _ in range(3):
            start = monotonic()
            child = request("fork", **parent, at=seq)
            samples.append(round(1000*(monotonic()-start), 2))
            assert request("inspect", run=child["run"])["origin"]["run"] == parent["run"]
        with db.connect() as conn:
            # Logical payload only. Indexes, row headers, WAL and compression are separate.
            row = conn.execute("SELECT sum(octet_length(record::text)) AS bytes FROM il_events WHERE run=%s",
                               (child["run"],)).fetchone()
        print(json.dumps(dict(nodes=length, source_events=seq, preview_fork_ms=samples,
                              child_event_json_bytes=row["bytes"])))


def live_restore(request, token, source, restored, root):
    graph = {"nodes": [{"id": "review", "kind": "human"}], "inputs": {"x": "review.request"},
             "outputs": {"approved": "review.answer"}}
    run = request("start", graph=graph, inputs={"x": "review the backup"})
    assert work_one(source, "original")["state"] == "waiting"
    task, = [t for t in request("tasks") if t["run"] == run["run"]]
    ready, stop = threading.Event(), threading.Event()
    committed = []

    def publish():
        while not stop.is_set():
            n = len(committed) + 1
            body = base64.b64encode(f"revision {n}".encode()).decode()
            request("put", namespace="shared", name="live", file=body)
            committed.append(monotonic())
            ready.set()
            stop.wait(.005)

    with ThreadPoolExecutor(1) as pool:
        writer = pool.submit(publish)
        try:
            if not ready.wait(10):
                raise RuntimeError("writer did not commit")
            begin = monotonic()
            backup(root / "backup")
            end = monotonic()
        finally:
            stop.set()
        writer.result()
    overlap = sum(begin < t < end for t in committed)
    assert overlap, "no writes overlapped backup"
    # A marker committed after snapshot completion must be absent from the restored DB.
    request("put", namespace="shared", name="after-backup", data=True)
    start = monotonic()
    archive = root / "backup"
    subprocess.run(["sha256sum", "--check", "--quiet", "SHA256SUMS"], cwd=archive, check=True)
    subprocess.run(["pg_restore", "--exit-on-error", "--single-transaction", "--no-owner",
                    "-d", restored, str(archive / "database.dump")], check=True)
    shutil.copytree(archive / "content", root / "restored")
    os.environ["INTERLACING_CONTENT_DIR"] = str(root / "restored")
    target = Database("dbname=" + restored)
    service = Collaboration(target)

    def read(action, **args):
        return service.request(token, action, args)

    with target.connect() as conn:
        versions = [r["version"] for r in conn.execute(
            "SELECT version FROM il_resources WHERE name='live' ORDER BY version")]
        assert not conn.execute("SELECT 1 FROM il_resources WHERE name='after-backup'").fetchone()
    assert versions == list(range(1, len(versions)+1)) and versions
    for n in versions:
        data = read("get", namespace="shared", name="live", version=n)
        assert base64.b64decode(data["file"]) == f"revision {n}".encode()
    pending, = [t for t in read("tasks") if t["run"] == run["run"]]
    assert pending["id"] == task["id"]
    claim = read("claim", task=pending["id"], version=pending["version"])
    read("submit", **claim, answer="ok")
    assert work_one(target, "replacement")["state"] == "done"
    assert read("status", **run)["result"]["outputs"] == {"approved": ["ok"]}
    print(json.dumps(dict(backup_ms=round(1000*(end-begin), 2), writes_during_backup=overlap,
                          latest_restored_version=versions[-1], latest_source_version=len(committed),
                          restored_and_review_completed_ms=round(1000*(monotonic()-start), 2))))


def demo(token, source):
    """Drive the documented example through the installed HTTP service and worker."""
    example = Path(__file__).resolve().parents[1] / "examples" / "review"
    env = {**os.environ, "INTERLACING_POSTGRES_DSN": "dbname=" + source}
    command = [sys.executable, "-m", "interlacing.cli", "collab"]
    with subprocess.Popen(command + ["serve", "--port", "0"], env=env, stdout=subprocess.PIPE,
                          text=True) as http:
        try:
            url = json.loads(http.stdout.readline())["address"]
            with urlopen(url + "/review", timeout=5) as page:
                assert page.status == 200 and "text/html" in page.headers["Content-Type"]
            lead = Client(url, token)
            people = {name: Client(url, lead.request("member", name=name)["token"]) for name in ("Alice", "Bob")}

            def example_run(*args):
                output = subprocess.check_output([sys.executable, "model_review.py", *args], cwd=example,
                                                 env={**env, "MANAGER_TOKEN": token, "INTERLACING_URL": url},
                                                 text=True, timeout=30)
                return literal_eval(output)["run"]

            def step():
                subprocess.run(command + ["worker", "--once", "--allow", "model_review"], env=env,
                               cwd=example, stdout=subprocess.DEVNULL, timeout=30, check=True)

            def answer(run, name, node, value):
                client = people[name]
                task, = [t for t in client.request("tasks") if t["run"] == run and t["node"] == node]
                detail = client.request("review", task=task["id"])
                client.request("submit", task=task["id"], version=task["version"], answer=value)
                return detail

            original = example_run()
            step()
            answer(original, "Alice", "q1", 9)
            answer(original, "Alice", "q3", 5)
            step()
            answer(original, "Bob", "q2", 18)
            step()
            branch = example_run("--revise", original, "--value", "12")
            step()
            detail = answer(branch, "Bob", "q2", 24)
            assert detail["task"]["request"] == 12 and any(c["previous"] == 9 for c in detail["changes"])
            step()
            for run, expected in ((original, 18), (branch, 24)):
                assert lead.request("status", run=run)["result"]["outputs"] == {
                    "report": [{"forecast": expected, "baseline": 5}]}
            print("HTTP demo: original 18/5, revised 24/5, reviewer compared 9 and 12.")
        finally:
            http.terminate()


def main():
    for tool in ("createdb", "dropdb", "pg_dump", "pg_restore", "sha256sum"):
        if shutil.which(tool) is None:
            raise SystemExit(f"missing executable: {tool}")
    if sys.flags.optimize:
        raise SystemExit("run with Python assertions enabled")
    with scratch_database() as source, scratch_database() as restored, TemporaryDirectory() as temp:
        root = Path(temp)
        (root / "content").mkdir()
        os.environ.update(PGDATABASE=source, INTERLACING_CONTENT_DIR=str(root/"content"))
        db = Database("")
        db.install()
        service = Collaboration(db)
        token = service.bootstrap("rehearsal", "manager")["token"]

        def request(action, **args):
            return service.request(token, action, args)

        demo(token, source)
        for processes in (1, 2):
            capacity(request, processes)
        history_cost(request, db)
        live_restore(request, token, db, restored, root)
        # createdb/dropdb use a separate maintenance database.
        os.environ.pop("PGDATABASE", None)


if __name__ == "__main__":
    main()
