"""Read a committed PostgreSQL position and create a separate run from its root prefix."""
from secrets import token_hex
from uuid import UUID, uuid4

from ..engine.durable import DurableState
from ..engine.wiring import INPUT
from ..runs import fork_inputs
from ..spec import SpecError, load
from .database import Refused, encoded


def source_run(db, project, run):
    row = db.execute("SELECT * FROM il_runs WHERE id=%s AND project=%s",
                     (UUID(str(run)), project)).fetchone()
    if not row:
        raise Refused("run unavailable")
    return row


def positions(db, actor, run):
    source_run(db, actor["project"], run)
    last = db.execute("SELECT COALESCE(MAX(seq),0) AS n FROM il_events WHERE run=%s AND scope='[]'",
                      (run,)).fetchone()["n"]
    points = db.execute("SELECT seq FROM il_checkpoints WHERE run=%s AND scope='[]' ORDER BY seq", (run,))
    return {"last_event": last, "checkpoints": [r["seq"] for r in points]}


def fork_run(db, project, run, at, inputs, execute):
    if type(at) is not int or at < 1 or type(execute) is not bool:
        raise ValueError("provide a positive event number and a boolean execute")
    if inputs is not None and not isinstance(inputs, dict):
        raise ValueError("inputs must be an object")
    overrides = inputs or {}
    source = source_run(db, project, run)
    if not db.execute("SELECT 1 FROM il_events WHERE run=%s AND scope='[]' AND seq=%s", (run, at)).fetchone():
        raise ValueError("event position outside run history")

    state = DurableState()
    resources = source["resources"].copy()
    saved_inputs = {}
    header = None
    revisions = set()
    with db.cursor(name="fork_prefix") as cursor:
        cursor.execute("SELECT record FROM il_events WHERE run=%s AND scope='[]' AND seq<=%s ORDER BY seq", (run, at))
        for item in cursor:
            record = item["record"]
            state.apply(record)
            kind = record["kind"]
            if kind == "run":
                header = record
                resources = record.get("resource_refs", resources).copy()
            if kind in ("run", "fork"):
                for edge, _, _, body, *_ in record["sent"]:
                    name = edge.removeprefix(INPUT)
                    saved_inputs[name] = body[1]
                    if kind == "fork":
                        resources.pop(name, None)
            if kind == "correct":
                revisions.update(packed[5] for packed, route in zip(record["sent"], record["routes"]) if route == "rerun")
    if state.pending:
        raise ValueError(f"event {at} has unfinished activations: {', '.join(sorted(state.pending))}")
    if header is None:
        raise ValueError("run has no committed header")
    description = source["description"]
    if overrides and description is None:
        # Runs written before schema v2 did not retain the compiled port schemas.
        try:
            description = load(source["graph"]).describe()
        except SpecError as exc:
            raise ValueError("legacy run lacks compiled input schemas for these overrides") from exc
    sent = fork_inputs(description, state, overrides) if overrides else []
    branch = str(uuid4())
    origin = {"run": str(run), "event": at, "run_id": header["id"], "scope_id": header["scope_id"]}
    fresh = {**header, "id": token_hex(8), "scope_id": token_hex(16),
             "resource_refs": header.get("resource_refs", source["resources"])}
    marker = {"schema_version": 1, "kind": "fork", "origin": origin, "sent": sent}
    saved_inputs.update(overrides)
    for name in overrides:
        resources.pop(name, None)
    db.execute("INSERT INTO il_runs(id,project,graph,inputs,resources,assignments,state,workers,description,origin) "
               "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
               (branch, project, encoded(source["graph"]), encoded(saved_inputs), encoded(resources),
                encoded(source["assignments"]), "queued" if execute else "preview", source["workers"],
                encoded(description) if description is not None else None, encoded(origin)))
    db.execute("INSERT INTO il_events VALUES (%s,'[]',1,%s)", (branch, encoded(fresh)))
    db.execute("INSERT INTO il_events SELECT %s,scope,seq,record FROM il_events "
               "WHERE run=%s AND scope='[]' AND seq BETWEEN 2 AND %s", (branch, run, at))
    db.execute("INSERT INTO il_events VALUES (%s,'[]',%s,%s)", (branch, at+1, encoded(marker)))
    state.data["run"]["id"] = fresh["id"]
    state.apply(marker)
    db.execute("INSERT INTO il_checkpoints VALUES (%s,'[]',%s,%s)", (branch, at+1, encoded(state.data)))
    for delivery in revisions:
        db.execute("INSERT INTO il_revisions VALUES (%s,'[]',%s)", (branch, delivery))
    if execute:
        db.execute("INSERT INTO il_commands(run,key) VALUES (%s,'start')", (branch,))
    return {"run": branch, "origin": origin, "executable": execute}


def _archived(db, run, scope):
    name, turn = scope[-1]
    parent = scope[:-1]
    logs = db.execute("SELECT record FROM il_events WHERE run=%s AND scope=%s AND record->>'kind'='act' "
                      "AND record->>'node'=%s AND record->>'round'=%s ORDER BY seq DESC",
                      (run, encoded(parent), name, str(turn))).fetchall()
    records = [r["record"] for r in logs]
    if not records and parent:
        records = [r for r in reversed(_archived(db, run, parent))
                   if r["kind"] == "act" and r["node"] == name and r["round"] == turn]
    for record in records:
        for child in record.get("child_history", ()):
            if any(e["kind"] == "run" and str(e.get("run_id")).endswith(f"/{name}#{turn}") for e in child):
                return child
    return []


def read_history(db, actor, run, after=0, limit=100, scope=None):
    if type(after) is not int or after < 0 or type(limit) is not int or not 1 <= limit <= 1000:
        raise ValueError("invalid history range")
    scope = [] if scope is None else scope
    if not isinstance(scope, list) or any(not isinstance(p, list) or len(p) != 2 or not isinstance(p[0], str)
                                          or type(p[1]) is not int or p[1] < 1 for p in scope):
        raise ValueError("scope must be a list of [node, positive round] pairs")
    source_run(db, actor["project"], run)
    rows = db.execute("SELECT seq,record FROM il_events WHERE run=%s AND scope=%s AND seq>%s ORDER BY seq LIMIT %s",
                      (run, encoded(scope), after, limit)).fetchall()
    if rows or not scope:
        return rows
    archive = _archived(db, run, scope)
    return [{"seq": e["seq"], "record": e, "archived": True} for e in archive if e["seq"] > after][:limit]


def inspect_run(db, actor, run, at=None):
    source = source_run(db, actor["project"], run)
    last = db.execute("SELECT COALESCE(MAX(seq),0) AS seq FROM il_events WHERE run=%s AND scope='[]'",
                      (run,)).fetchone()["seq"]
    if at is not None and (type(at) is not int or not 1 <= at <= last):
        raise ValueError("event position outside run history")
    through = last if at is None else at
    snapshot = db.execute("SELECT state FROM il_checkpoints WHERE run=%s AND scope='[]' AND seq<=%s "
                          "ORDER BY seq DESC LIMIT 1", (run, through)).fetchone()
    state = DurableState(snapshot["state"] if snapshot else None)
    for row in db.execute("SELECT record FROM il_events WHERE run=%s AND scope='[]' AND seq BETWEEN %s AND %s "
                          "ORDER BY seq", (run, state.seq+1, through)):
        state.apply(row["record"])
    return {"event": through, "current_status": source["state"], "origin": source["origin"],
            "forkable": bool(through) and not state.pending, "pending": {
                node: {"round": item["round"], "took": item["took"]} for node, item in state.pending.items()},
            "rework": state.rework.jobs(), "outputs": {name: [body[1] if body[0] == "v" else
             {"digest": body[1], "size": body[2], "media": body[3]} for body in bodies]
             for name, bodies in state.data["outputs"].items()}}
