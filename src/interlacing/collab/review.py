"""Show the corrections actually consumed by one visible human task."""
from uuid import UUID

from .database import Refused


def review_task(db, actor, task):
    item = db.execute("SELECT t.id::text,t.run::text,t.node,t.turn,t.request,t.answer_schema,t.state,"
                      "t.assignee,t.version,r.origin FROM il_tasks t JOIN il_runs r ON r.id=t.run "
                      "WHERE t.id=%s AND r.project=%s AND (%s='manager' OR t.assignee=%s OR t.state='open')",
                      (UUID(str(task)), actor["project"], actor["role"], actor["name"])).fetchone()
    if not item:
        raise Refused("task unavailable")
    origin = item.pop("origin") or {}
    # A binding records delivery IDs, including the scope, so unrelated forks/edits never match.
    rows = db.execute("""
        SELECT e.record->>'node' AS source, e.record->'external' AS external,
               e.record->'version' AS message_version, prior.request AS previous,
               prior.id IS NOT NULL AS can_compare
        FROM il_task_reads b
        JOIN il_events e ON (e.run,e.scope)=(b.run,b.scope) AND e.record->>'kind'='correct'
        CROSS JOIN LATERAL json_array_elements(e.record->'sent') AS sent(p)
        LEFT JOIN LATERAL (
            SELECT t.id,t.request FROM il_task_reads old JOIN il_tasks t ON t.id=old.task
            WHERE old.run IN (b.run,%s::uuid) AND old.scope=b.scope AND old.delivery=p->>5
              AND t.node=%s AND (%s='manager' OR t.assignee=%s)
            ORDER BY (old.run=b.run) DESC,t.turn DESC LIMIT 1
        ) prior ON true
        WHERE b.task=%s AND b.delivery=((p->>0)||'#'||(p->>1))
        ORDER BY e.seq
    """, (origin.get("run"), item["node"], actor["role"], actor["name"], task)).fetchall()
    result = {"task": item, "changes": rows}
    if actor["role"] == "manager" and origin:
        result["impact"] = fork_activity(db, item["run"], origin["event"])
    return result


def fork_activity(db, run, at):
    """Group actual root events on the two sides of this fork position."""
    nodes = db.execute("""
        SELECT record->>'node' AS node,
               bool_or(seq<=%s AND record->>'kind'='act') AS before,
               bool_or(seq>%s AND record->>'kind'='bind') AS activated,
               bool_or(seq>%s AND record->>'kind'='correct') AS revised
        FROM il_events WHERE run=%s AND scope='[]' AND record->>'kind' IN ('act','bind','correct')
        GROUP BY record->>'node' ORDER BY node
    """, (at, at, at, run)).fetchall()
    return {"activated": [n["node"] for n in nodes if n["activated"]],
            "revised": [n["node"] for n in nodes if n["revised"]],
            "carried": [n["node"] for n in nodes if n["before"] and not n["activated"] and not n["revised"]]}
