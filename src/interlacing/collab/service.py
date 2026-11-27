"""Authenticated project operations. Each request uses one PostgreSQL transaction."""
from __future__ import annotations

import base64
import hashlib
import secrets
from uuid import UUID, uuid4

from ..base import Ref, canonical, digest
from ..spec.kinds import schema_of
from .review import review_task
from .history import fork_run, inspect_run, positions, progress_page, read_history
from .validation import validate_submission
from .database import Database, Refused, RequestConflict, encoded


def audit(db, project, actor, action, subject, details=None):
    db.execute("INSERT INTO il_audit(project,actor,action,subject,details) VALUES (%s,%s,%s,%s,%s)",
               (project, actor, action, str(subject), encoded(details or {})))


def _secret(token):
    if not isinstance(token, str) or not token:
        raise Refused("member credentials required")
    return hashlib.sha256(token.encode()).hexdigest()


def _member(db, project, name, role):
    if not isinstance(name, str) or not name.strip() or role not in ("manager", "member"):
        raise Refused("provide a member name and valid role")
    token = secrets.token_urlsafe(32)
    inserted = db.execute("INSERT INTO il_members VALUES (%s,%s,%s,%s) ON CONFLICT(project,name) DO NOTHING RETURNING name",
                          (project, name, _secret(token), role)).fetchone()
    if not inserted:
        raise Refused("member already exists")
    return {"name": name, "token": token}


class Collaboration:
    def __init__(self, database: Database):
        self.database = database

    def bootstrap(self, project, manager):
        """Trusted operator operation. Create the project and its first manager together."""
        if not project:
            raise Refused("provide a project name")
        with self.database.connect() as db:
            if not db.execute("INSERT INTO il_projects VALUES (%s) ON CONFLICT DO NOTHING RETURNING project",
                              (project,)).fetchone():
                raise Refused("project already exists")
            result = _member(db, project, manager, "manager")
            audit(db, project, manager, "project.create", project)
            return result

    def request(self, token, action, args):
        with self.database.connect() as db:
            if action in ("history", "progress", "inspect", "checkpoints", "fork", "review"):
                db.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            actor = db.execute("SELECT project,name,role FROM il_members WHERE secret=%s FOR SHARE",
                               (_secret(token),)).fetchone()
            if not actor:
                raise Refused("unknown member credentials")
            if action == "whoami":
                return dict(actor)
            operations = {"tasks": self.tasks, "review": review_task, "claim": self.claim, "submit": self.submit,
                          "put": self.put, "get": self.get, "resources": self.resources}
            if actor["role"] == "manager":
                operations.update(member=self.member, start=self.start, status=self.status, assign=self.assign,
                                  audit=self.audit_log, retry=self.retry, history=read_history, progress=progress_page, inspect=inspect_run, validate=self.validate,
                                  fork=self.fork, checkpoints=positions)
            if action not in operations:
                raise Refused("operation unavailable to this member")
            return operations[action](db, actor, **args)

    @staticmethod
    def member(db, actor, name, role="member"):
        result = _member(db, actor["project"], name, role)
        audit(db, actor["project"], actor["name"], "member.add", name, {"role": role})
        return result

    @staticmethod
    def _namespace(actor, namespace):
        if namespace not in ("shared", "personal"):
            raise Refused("choose shared or personal")
        return "shared" if namespace == "shared" else "user/" + actor["name"]

    def put(self, db, actor, namespace, name, data=None, file=None):
        if not isinstance(name, str) or not name:
            raise Refused("provide a resource name")
        project = actor["project"]
        if namespace == "shared" and actor["role"] != "manager":
            raise Refused("shared resource publication requires a manager")
        space = self._namespace(actor, namespace)
        value = {"data": data}
        if file is not None:
            if data is not None:
                raise Refused("choose a file or JSON data")
            content = base64.b64decode(file, validate=True)
            ref = self.database.content.put(content)
            db.execute("INSERT INTO il_files VALUES (%s,%s,%s) ON CONFLICT DO NOTHING", (project, ref.digest, ref.size))
            value = {"file": ref.digest, "size": ref.size}
        db.execute("SELECT project FROM il_projects WHERE project=%s FOR UPDATE", (project,))
        row = db.execute("INSERT INTO il_resources SELECT %s,%s,%s,COALESCE(MAX(version),0)+1,%s "
                         "FROM il_resources WHERE project=%s AND namespace=%s AND name=%s RETURNING version",
                         (project, space, name, encoded(value), project, space, name)).fetchone()
        audit(db, project, actor["name"], "resource.put", name, {"namespace": space, **row})
        return {"namespace": namespace, "name": name, **row}

    def resources(self, db, actor, namespace, prefix="", query="", after=None, limit=100):
        if not isinstance(prefix, str) or not isinstance(query, str) or type(limit) is not int or not 1 <= limit <= 500:
            raise ValueError("provide text prefix/query and a limit from 1 to 500")
        if after is None:
            after = ["", 0]
        if (not isinstance(after, list) or len(after) != 2 or not isinstance(after[0], str)
                or type(after[1]) is not int or after[1] < 0):
            raise ValueError("after must be [name, version]")
        space = self._namespace(actor, namespace)
        rows = db.execute("SELECT name,version,CASE WHEN data->>'file' IS NULL THEN 'json' ELSE 'file' END AS kind "
                          "FROM il_resources WHERE project=%s AND namespace=%s AND starts_with(name,%s) AND "
                          "(strpos(lower(name),lower(%s))>0 OR strpos(lower(COALESCE((data->'data')::text,'')),lower(%s))>0) "
                          "AND (name,version)>(%s,%s) ORDER BY name,version LIMIT %s",
                          (actor["project"], space, prefix, query, query, *after, limit+1)).fetchall()
        return {"items": rows[:limit], "next": [rows[limit-1]["name"], rows[limit-1]["version"]]
                if len(rows) > limit else None}

    def _resource(self, db, actor, namespace, name, version):
        space = self._namespace(actor, namespace)
        row = db.execute("SELECT data FROM il_resources WHERE project=%s AND namespace=%s AND name=%s AND version=%s",
                         (actor["project"], space, name, version)).fetchone()
        if row is None:
            raise Refused("resource version unavailable")
        return row["data"]

    def get(self, db, actor, namespace, name, version):
        value = self._resource(db, actor, namespace, name, version)
        if "file" in value:
            content = self.database.content.get(Ref(value["file"], value["size"]))
            return {"file": base64.b64encode(content).decode("ascii"), "sha256": value["file"]}
        return value

    @staticmethod
    def validate(db, actor, graph, workers=4, assignments=None):
        result = validate_submission(graph, workers, assignments)
        for member in set((assignments or {}).values()):
            if not db.execute("SELECT 1 FROM il_members WHERE project=%s AND name=%s",
                              (actor["project"], member)).fetchone():
                raise Refused("unknown task assignee")
        return result

    def start(self, db, actor, graph, inputs=None, resources=None, assignments=None, workers=4, request_key=None):
        if not isinstance(graph, dict) or any(value is not None and not isinstance(value, dict)
                                               for value in (inputs, resources, assignments)):
            raise ValueError("graph, inputs, resources and assignments must be objects")
        run = str(uuid4())
        if request_key is not None:
            if not isinstance(request_key, str) or not request_key.strip() or len(request_key) > 256:
                raise ValueError("request_key must contain 1 to 256 characters")
            fingerprint = digest([graph, inputs or {}, resources or {}, assignments or {}, workers])
            key = (actor["project"], actor["name"], request_key)
            inserted = db.execute("INSERT INTO il_start_requests VALUES (%s,%s,%s,%s,%s) "
                                  "ON CONFLICT DO NOTHING RETURNING run", (*key, fingerprint, run)).fetchone()
            if inserted is None:
                previous = db.execute("SELECT fingerprint,run FROM il_start_requests WHERE "
                                      "project=%s AND actor=%s AND request_key=%s", key).fetchone()
                if previous["fingerprint"] != fingerprint:
                    raise RequestConflict("request_key already used with different start arguments")
                return {"run": str(previous["run"])}
        self.validate(db, actor, graph, workers, assignments)
        inputs = dict(inputs or {})
        resources = resources or {}
        for port, ref in resources.items():
            if port in inputs:
                raise Refused("a graph input can have only one source")
            item = self._resource(db, actor, **ref)
            inputs[port] = item["data"] if "data" in item else item
        assignments = assignments or {}
        db.execute("INSERT INTO il_runs(id,project,graph,inputs,resources,assignments,workers,shared_workers) VALUES (%s,%s,%s,%s,%s,%s,%s,true)",
                   (run, actor["project"], encoded(graph), encoded(inputs), encoded(resources), encoded(assignments), workers))
        db.execute("INSERT INTO il_commands(run,key) VALUES (%s,'start')", (run,))
        audit(db, actor["project"], actor["name"], "run.start", run, {"resources": resources})
        return {"run": run}

    @staticmethod
    def fork(db, actor, run, at, inputs=None, execute=False, output=None):
        result = fork_run(db, actor["project"], run, at, inputs, execute, output=output, actor=actor["name"])
        audit(db, actor["project"], actor["name"], "run.fork", result["run"],
              {"parent": run, "event": at, "execute": execute})
        return result

    @staticmethod
    def status(db, actor, run):
        row = db.execute("SELECT state,result,generation,resources,workers,origin FROM il_runs WHERE id=%s AND project=%s",
                         (UUID(str(run)), actor["project"])).fetchone()
        if not row:
            raise Refused("run unavailable")
        return row

    @staticmethod
    def retry(db, actor, run):
        row = db.execute("SELECT generation,state FROM il_runs WHERE id=%s AND project=%s FOR UPDATE",
                         (UUID(str(run)), actor["project"])).fetchone()
        if not row or row["state"] != "failed":
            raise Refused("retry requires a failed run")
        db.execute("INSERT INTO il_commands(run,key) VALUES (%s,%s)", (run, "retry/" + str(row["generation"])))
        db.execute("UPDATE il_runs SET state='queued' WHERE id=%s", (run,))
        audit(db, actor["project"], actor["name"], "run.retry", run)
        return {"run": run}

    @staticmethod
    def tasks(db, actor):
        return db.execute("SELECT t.id::text,t.run::text,t.node,t.turn,t.request,t.answer_schema,t.state,t.assignee,t.version "
                          "FROM il_tasks t JOIN il_runs r ON r.id=t.run WHERE r.project=%s AND "
                          "(%s='manager' OR t.assignee=%s OR t.state='open') ORDER BY r.created,t.node,t.turn",
                          (actor["project"], actor["role"], actor["name"])).fetchall()

    @staticmethod
    def _task(db, actor, task):
        args = (UUID(str(task)), actor["project"])
        row = db.execute("SELECT r.id,r.state FROM il_runs r JOIN il_tasks t ON t.run=r.id "
                         "WHERE t.id=%s AND r.project=%s FOR UPDATE OF r", args).fetchone()
        if row is None:
            raise Refused("task unavailable")
        return db.execute("SELECT * FROM il_tasks WHERE id=%s FOR UPDATE", (task,)).fetchone(), row["state"]

    def claim(self, db, actor, task, version):
        return self._assign(db, actor, task, version, actor["name"], "claim")

    def assign(self, db, actor, task, version, member):
        if not db.execute("SELECT 1 FROM il_members WHERE project=%s AND name=%s",
                          (actor["project"], member)).fetchone():
            raise Refused("unknown project member")
        return self._assign(db, actor, task, version, member, "assign")

    def _assign(self, db, actor, task, version, member, action):
        row, state = self._task(db, actor, task)
        if (type(version) is not int or state in ("done", "failed") or row["version"] != version
                or row["state"] not in ("open", "claimed")):
            raise Refused("task state or version changed")
        if action == "claim" and row["state"] != "open":
            raise Refused("task already claimed")
        db.execute("UPDATE il_tasks SET state='claimed',assignee=%s,version=version+1 WHERE id=%s", (member, task))
        audit(db, actor["project"], actor["name"], "task." + action, task, {"member": member, "version": version+1})
        return {"task": task, "version": version+1}

    def submit(self, db, actor, task, version, answer):
        row, state = self._task(db, actor, task)
        if type(version) is not int or row["assignee"] != actor["name"]:
            raise Refused("submission requires the assigned member")
        if row["state"] == "submitted":
            if row["submitted_version"] == version and canonical(row["answer"]) == canonical(answer):
                return {"task": task, "version": row["version"]}
            raise Refused("task already submitted with a different version or answer")
        if state in ("done", "failed") or row["state"] != "claimed" or row["version"] != version:
            raise Refused("task state or version changed")
        schema_of(row["answer_schema"]).check(answer)
        db.execute("UPDATE il_tasks SET state='submitted',submitted_version=version,version=version+1,answer=%s WHERE id=%s",
                   (encoded(answer), task))
        db.execute("INSERT INTO il_commands(run,key) VALUES (%s,%s)", (row["run"], "answer/" + task))
        db.execute("UPDATE il_runs SET state='queued' WHERE id=%s AND state='waiting'", (row["run"],))
        audit(db, actor["project"], actor["name"], "task.submit", task, {"version": version+1})
        return {"task": task, "version": version+1}

    @staticmethod
    def audit_log(db, actor):
        return db.execute("SELECT id,actor,action,subject,details,ts::text FROM il_audit WHERE project=%s ORDER BY id",
                          (actor["project"],)).fetchall()
