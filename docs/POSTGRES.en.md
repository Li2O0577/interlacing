# PostgreSQL collaboration service

**English** · [中文](POSTGRES.md)

A project can have multiple managers and members. Managers start runs, publish shared resources and assign tasks. Members claim open tasks or answer assigned ones using the output format declared by the human node. Each member also has a private space for resources. Workers share a database and content directory. Each worker advances one run at a time. Local JSONL and SQLite run directories use the regular CLI.

## Start the service

Use Python 3.11 or later. For the 0.17.0 release, install `python -m pip install "interlacing[postgres]==0.17.0"`. For the request keys, browser review page and nested parallelism described here, run `python -m pip install -e ".[postgres]"` in the current checkout. Set `INTERLACING_POSTGRES_DSN` for both the service and the workers. Use a dedicated PostgreSQL database and an account with permission to create tables during initialization. For files or artifact edges, set `INTERLACING_CONTENT_DIR` to the same shared directory for all processes. Clients authenticate with project member tokens.

```sh
interlacing collab init
interlacing collab project research lead
interlacing collab serve --port 8765
```

The second command prints the first manager's token. The API listens on the loopback interface. For remote access, use an HTTPS reverse proxy or an authenticated tunnel. In another terminal run `interlacing collab worker`. You can start multiple workers. Each claims one queued command and renews its lease automatically. Use `--once` to handle at most one command and exit. Use `--allow MODULE` to expose trusted Python functions and `--models` to enable model providers configured on the host. A manager's workflows can use the capabilities enabled on their workers.

## Complete a human review

Send each line below as a separate JSON request to `POST /v1/request` with `Authorization: Bearer <token>`. Substitute the returned task UUID, version and run UUID where indicated.

```jsonl
{"action":"member","args":{"name":"Alice"}}
{"action":"start","args":{"graph":{"nodes":[{"id":"approve","kind":"human","outputs":[{"name":"answer","schema":"int"}]}],"inputs":{"quote":"approve.request"},"outputs":{"accepted":"approve.answer"}},"inputs":{"quote":"Confirm the quote"},"assignments":{"approve":"Alice"}}}
{"action":"tasks"}
{"action":"submit","args":{"task":"<task UUID>","version":1,"answer":7}}
{"action":"status","args":{"run":"<run UUID>"}}
```

Use the manager token to create Alice, start the run and check its final status. Alice uses the token returned by `member` to list her tasks and submit her answer with the task's current version. Once the worker resumes the run, the manager should see `[7]` in `result.outputs.accepted`. Omit `assignments` to open a task to members. The member calls `claim`, then uses the returned version to submit. Independent human nodes can wait for different members at the same time.

An assignment key identifies the exact node and enclosing round. For example, `block.2/approve` assigns the nested approval in round two. Other rounds follow their own assignments. Pass the same `assignments` to `validate` and `start`. The service checks member names and explicitly declared node paths. It reports the offending key for a typo, an invalid round or a target that is a regular program node. When checking a path requires graph expansion, such as a node repeated with `times`, preflight includes `assignment_paths` in `worker_checks`. The worker checks the compiled graph before executing the first node or opening a task. Read `status` for the reason if this fails.

The number of rounds actually executed depends on the run. Unassigned tasks are open for claims. A node added later by a graph edit starts with an open task, which a manager can reassign with `assign`. Declare a human node in advance when only a specific member should see its first task. After an upgrade, inspect existing assignments and use `assign` to change them if needed.

| Action | Who can call it | Inputs and result |
|---|---|---|
| `member` | Manager | Supply `name` and optional `role`. Returns a token for the new member or manager. |
| `start`, `status`, `retry` | Manager | Create a run, check its state by UUID or retry a failed run. |
| `history`, `audit` | Manager | Read events in a run scope (root by default) or the project's audit records. Page through history with `after` and `limit`. |
| `tasks`, `claim` | Member or manager | Members see open tasks and their own assignments. Managers see the project's tasks. Claim with the task UUID and current version. |
| `assign` | Manager | Assign or transfer a task with `task`, `version` and `member`. The previous version expires. |
| `submit` | Current assignee | Supply `task`, `version` and `answer`. Retrying the same version and answer returns the original receipt version. |
| `put`, `get` | Member or manager | Supply `namespace` (`shared` or `personal`) and `name`. Write JSON in `data` or base64 bytes in `file`. Read a particular `version`. Only managers publish shared resources. |

Use `start.resources` to bind a graph input to a namespace, name and version. The run receives the JSON value fixed at creation, or a reference containing the file's digest and size. Members use `get` to retrieve files they can access. File and artifact bytes live in the content store. PostgreSQL holds the references, versions and access rules. Personal resources are visible only to their owner. The HTTP request limit is 16 MiB including base64 overhead.

## Parallel execution on the server

New runs default to four concurrent node activations across the graph. Set `start.workers` from 0 to 32. Set it to 0 for serial execution. A single lease holder advances each run. Multiple worker processes can advance different runs. To upgrade an existing database, stop the old services and workers, run `interlacing collab init` with the new version, then start the new processes. Initialization is repeatable. Existing runs and their forks keep their scheduling settings.

Each `from → to` edge tells the downstream node which output to read. Independent nodes can execute concurrently. A node with dependencies starts according to its input ports' readiness rules. For example, a fast branch can deliver a committed output to a reviewer while a slow branch is still executing. The worker can accept the answer and advance the fast branch. If scheduling has already stopped, a durable resume command starts the next scheduling pass.

The root graph and nested blocks share the run's concurrency limit. Ready scopes receive slots in request order. Nodes waiting for a subgraph or a challenge reply release their slots. A block's `workers` setting can further restrict that block. The run limit counts executing nodes, including coroutines. Coordination threads, threads created by business functions and threads still exiting after a timeout are counted separately.

Durable graphs support `value` and `artifact` edges, both with optional `challenges`. A manager can call `validate(graph, workers)` to see which checks passed and which require a worker. `start` performs the same preflight. For explicitly declared `stream` and `call` edges it reports their location before enqueueing. The worker checks function signatures, model configuration, the compiled graph and run inputs against its allowed catalog.

## Challenges, replies and worker takeover

A quote producer can allow the reviewer to challenge a delivery twice:

```json
{"from": "quote.out", "to": "review.price", "challenges": 2}
```

The reviewer calls `ctx.reject("price", "Does this quote include tax?")`, or awaits `ctx.areject(...)`. An explanation returns `explained` and the explanation text. A correction returns `corrected` and the new value to the reviewer waiting on that version. Other nodes that consumed the old value follow their correction policies. An exhausted challenge budget returns `refused` and a reason. Human decisions use explicit human nodes. Their tasks can be assigned, claimed and answered as usual.

If a worker exits after committing a challenge, its replacement can locate the question by run, nested scope, edge and delivery number. The full `challenge` event records the message version. An explanation produces a `review_reply` record. A correction records the question and decision in `answers`, committing the revision and reply in the same transaction. Artifact edges keep the same content directory. These records use the existing tables. Stop old workers and deploy matching service and worker versions together. Older workers refuse durable graphs with challenges.

Recovery may re-enter an unfinished business function or reply callback. Use a stable business idempotency key for external effects such as charging an account or sending a message. Resident nodes that call `ctx.receive()` during an activation use standard mode. [Worker takeover and multi-user review tests](../tests/test_postgres_collab.py) · [Recovery and artifact tests](../tests/test_durable_reviews.py)

## Python client and node retries

The client uses the Python standard library. Supply an existing run ID and manager token in this example. The HTTP endpoint is `POST /v1/request`. Errors include readable `error` text and a machine-readable `code`. The client's `ServiceError` exposes `status` and `code`.

```python
from interlacing.collab.client import Client, ServiceError

client = Client("http://127.0.0.1:8765", manager_token)
try:
    current = client.request("inspect", run=run_id)
    for event in client.events(run_id, after=current["event"], follow=True):
        print(event["seq"], event["record"]["kind"])
except ServiceError as error:
    print(error.status, error.code, str(error))
```

A synchronous function node that calls a business API can set `retry_attempts` from 1 to 16 (default 1). For example, `{"id":"price","kind":"function","function":"shop.quote","retry_attempts":3}` calls a function with ordinary inputs and a JSON result. By default, `ConnectionError` and `TimeoutError` trigger exponential backoff. Other errors are reported immediately. The journal records each completed attempt with the activation. A replacement worker inherits the attempt count and backoff deadline. Pass an idempotency key to external services to cover a crash between the external response and the local journal write. Async functions and functions taking `ctx` manage their own retries.

Select timeout retries with `"retry_on": ["timeout"]`, connection retries with `"connection"` in the list, or select both. When a manager calls `retry` on a failed run, an activation that exhausted its budget can call the function again. The database command ID distinguishes this authorization from a worker takeover. Successfully recorded calls keep their original results.

Pass an optional `request_key` of 1 to 256 characters to `start`. A retry by the same manager in the same project with the same key and arguments returns the original run UUID. Conflicting arguments return HTTP 409. Use a fresh key for an intentional second run. Omitting the key creates a run on every request.

## Follow a root run live

Managers can use `Client.stream_progress` to receive run state, activation starts (`bind`), commits (`act`), deliveries, corrections and custom stages. A synchronous or asynchronous business node can report a stage with `ctx.progress("Quote checked; preparing the review summary")`. Stage text is visible to project managers. A recovered activation may report the same stage again.

```python
cursor = 0
for frame in client.stream_progress(run_id, after=cursor):
    if frame["type"] == "event":
        print(frame["seq"], frame["kind"], frame.get("node"), frame.get("message"))
        cursor = frame["seq"]  # Persist after processing; pass it back on reconnect.
    else:
        print(frame["state"])
```

`GET /v1/progress?run=UUID&after=N` uses Bearer authentication and streams newline-delimited JSON. Sequence numbers match root history. Status frames include the current state and cursor, with an idle heartbeat roughly every five seconds. The connection closes after the last page of a terminal run. Each subscription reads committed events independently, checking every 250 ms after it catches up. Disconnecting a subscriber leaves the run active. Use paginated history for nested activations. This endpoint reports workflow progress, down to node stages; it does not stream model tokens.

## Inspect, fork and find resources

| Action | Inputs and result |
|---|---|
| `checkpoints`, `inspect` (manager) | Supply `run` to list the latest root event and retained checkpoints. `inspect(run, at)` returns outputs, pending activations, pending rework and whether the position is forkable. `current_status` is the run's state at query time. |
| `fork` (manager) | Supply `run` and root event number `at`. Use `inputs` to replace unconsumed graph inputs or `output` to revise a node's message. The default creates an inspectable preview. Set `execute: true` to schedule further work in an independent run. The result includes the new UUID and origin. |
| `history` (manager) | Reads root history by default. Use a scope such as `[["block",1],["child",1]]` for a nested activation. Active scopes return events. Cleaned-up child journals return retained provenance with `archived: true`. Both paginate by `seq`. Use the client's `events` cursor to keep polling. |
| `resources` (member or manager) | Choose namespace `shared` or `personal`. Filter names by `prefix` or use `query` for a case-insensitive substring in names and JSON text. Files match by name. Results include name, version and kind. Pass `next` as `after` for another page. Use `get` to read a chosen version. |

A fork reconstructs committed state at the selected event. It preserves the origin, completed work and the input versions at that position. New tasks get independent IDs. Let unfinished activations settle or choose an earlier position. A legacy run without compiled port descriptions can be forked unchanged. Overriding inputs requires enough information to reconstruct port types. An executable fork can issue new external calls.

```python
# Replace the initial quote and inspect the preview origin.
preview = client.request("fork", run=run_id, at=1, inputs={"quote": "Revised quote"})
print(client.request("inspect", run=preview["run"])["origin"])
matches = client.request("resources", namespace="shared", query="quote", limit=20)
for item in matches["items"]:
    print(item["name"], item["version"])
```

### Revise an approved intermediate result

Read the target `deliver` record's `node` and `token` from `history`. The first delivery is version 1. Later `correct` records give subsequent versions. Choose a point after the delivery where `inspect` returns `forkable`. Here the quote node has emitted the message `price#1`.

```python
edit = {"node": "quote", "token": "price#1", "version": 1,
        "value": 120, "reason": "The supplier issued a tax-inclusive quote."}
branch = client.request("fork", run=run_id, at=position, output=edit, execute=True)
```

The fork records the manager and the reason, then routes the correction according to actual reads and graph correction policies. A downstream reviewer whose approval requires rework gets a new task. The parent keeps its original approval and history. Omit `execute` for an unscheduled preview. Locally, use durable SQLite with `Run.fork(..., output=edit)` or `interlacing fork --output '<JSON>'`. The local editor is recorded as `local`.

This operation supports JSON sent over ordinary forward value edges on a static root graph, including direct graph outputs. It needs a settled position, matching message version and port type, and a reason. Custom output validators, back edges and artifact deliveries fall outside its current scope. Use matching service and worker versions.

## Commits and takeover

Run leases use database time and increasing generations. Every transaction that writes a journal entry, checkpoint, task or completion state locks the run and verifies the generation. After the lease expires another worker can claim unfinished commands. Nodes resume from their graph commits. External writes rely on the business operation's idempotency mechanism.

A human answer, its resume command and its audit record commit in one transaction. Tasks are bound to a run, node path, activation round and version. When a revision requires a human node or its enclosing subgraph to run again, the message commit invalidates the affected task. The old task refuses further answers. The new task shows the revised request. A rejected submission leaves the task in its prior state. Back up the shared content directory with the database. The host supplies OS-level execution isolation and the cross-host content mount.

A single-host rehearsal restored PostgreSQL and file content to an independent database after stopping writes, read the original file and completed the pending task. Backup and restore under live writes were also verified on one host. See the [deployment guide](DEPLOY.en.md) for timings and the two-host acceptance procedure. [Historical verification notes (Chinese)](BRANCH_REVIEW.md)

Set `INTERLACING_TEST_POSTGRES` to a dedicated test database and run `python -m pytest -q tests/test_postgres_collab.py` to check worker processes, lease takeover, concurrent claims, transaction rollback and access to tasks and resources.

## Review a revised modeling result in the browser

After upgrading, run `interlacing collab init`. In a test project, have the manager create Alice and Bob as described above. From the repository root, enter `examples/review` and run `python -m interlacing.cli collab worker --allow model_review`. In another terminal in the same directory, set `MANAGER_TOKEN` and run `python model_review.py`. Keep the returned run UUID.

1. Alice and Bob open `/review` and enter their own tokens. Alice submits the integer 9 for q1 and 5 for the independent q3. Bob refreshes, reads the q1 result and submits 18 for q2.
2. Once the original run finishes, the manager runs `python model_review.py --revise ORIGINAL_RUN_UUID --value 12` to create an independent fork.
3. Bob refreshes and opens q2 in the new run. He can compare the old input 9 with the new input 12, see who changed it and why, and submit 24. The fork reuses Alice's q3. Its report contains 24 and 5. The original still contains 18 and 5.
4. A manager opening the forked task can refresh to see which root nodes started, were revised or carried forward an earlier result. A member sees revision details only for visible tasks. Previous inputs are shown only to someone with access to the corresponding old task. Tokens stay in page memory, so reloading the page requires signing in again.

The same task details are available through `review(task=UUID)`. Comparisons use the concrete deliveries and corrections bound to that task. Nested tasks also track their enclosing input dependencies.
