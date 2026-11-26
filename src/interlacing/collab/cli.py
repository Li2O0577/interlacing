"""Operator CLI and authenticated JSON-over-HTTP endpoint for project members."""
from __future__ import annotations

import json
import os
import socket
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from uuid import uuid4

from .database import Database, LostLease, Refused
from .service import Collaboration
from .worker import work_one


def arguments(commands):
    group = commands.add_parser("collab", help="PostgreSQL collaboration service")
    sub = group.add_subparsers(dest="operation", required=True)
    sub.add_parser("init", help="install the service tables")
    project = sub.add_parser("project", help="create a project and issue its first manager token")
    project.add_argument("project")
    project.add_argument("manager")
    serve = sub.add_parser("serve", help="serve project requests on a local HTTP port")
    serve.add_argument("--port", type=int, default=8765)
    worker = sub.add_parser("worker", help="execute queued runs")
    worker.add_argument("--once", action="store_true", help="claim at most one command and exit")
    worker.add_argument("--lease-seconds", type=int, default=30)
    worker.add_argument("--allow", action="append", default=[])
    worker.add_argument("--models", action="store_true", help="enable the host's configured model providers")


def server(database, port):
    service = Collaboration(database)

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format, *args):
            pass

        def reply(self, status, value):
            body = json.dumps(value, ensure_ascii=False, allow_nan=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self):
            if self.path != "/v1/request":
                return self.reply(404, {"error": "unknown endpoint"})
            scheme, _, token = self.headers.get("Authorization", "").partition(" ")
            if scheme.lower() != "bearer" or not token:
                return self.reply(401, {"error": "member credentials required"})
            try:
                size = int(self.headers.get("Content-Length", "0"))
                if not 0 < size <= 16 * 1024 * 1024:
                    return self.reply(413, {"error": "request requires a body of at most 16 MiB"})
                payload = json.loads(self.rfile.read(size))
                if not isinstance(payload, dict) or not isinstance(payload.get("args", {}), dict):
                    raise ValueError("expected an action and an object of arguments")
                result = service.request(token, payload["action"], payload.get("args", {}))
            except Refused as exc:
                return self.reply(403, {"error": str(exc)})
            except (TypeError, ValueError, KeyError) as exc:
                return self.reply(400, {"error": str(exc)})
            except database.driver.Error:
                return self.reply(503, {"error": "database request failed"})
            self.reply(200, result)

    return ThreadingHTTPServer(("127.0.0.1", port), Handler)


def dispatch(args, out):
    dsn = os.getenv("INTERLACING_POSTGRES_DSN")
    if not dsn:
        raise Refused("set INTERLACING_POSTGRES_DSN for the collaboration service")
    database = Database(dsn)
    if args.operation == "init":
        database.install()
        result = {"installed": True}
    elif args.operation == "project":
        result = Collaboration(database).bootstrap(args.project, args.manager)
    elif args.operation == "serve":
        with server(database, args.port) as http:
            print(json.dumps({"address": "http://127.0.0.1:" + str(http.server_port)}), file=out, flush=True)
            http.serve_forever()
        return 0
    else:
        worker = f"{socket.gethostname()}/{uuid4()}"
        while True:
            try:
                result = work_one(database, worker, seconds=args.lease_seconds, allow=args.allow, enable_models=args.models)
            except LostLease:
                result = {"state": "lease_lost"}
            if args.once:
                break
            if result is None:
                time.sleep(0.5)
    print(json.dumps(result), file=out)
    return 0
