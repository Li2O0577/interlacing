# interlacing · Python user and maintainer guide

[中文](HANDBOOK.zh-CN.md) · **English** · For **0.16.2**, Python 3.11+. Start with the first two examples if you are new. If you inherited a graph, start with the maintainer map at the end.

[Quick start](#start) · [Graphs and ordering](#graph) · [Edge protocols](#edges) · [Choosing nodes](#nodes) · [Composition](#compose) · [Execution and recovery](#run) · [Challenges and corrections](#review) · [JSON, CLI and MCP](#cli) · [Maintenance and troubleshooting](#maintain)

<a id="start"></a>

## 1 · Run a small dataflow

An experiment produces three readings. Compute their range and turn it into a sentence. Save the Python block below as a script and run it locally. Install the release with:

```sh
python -m pip install interlacing==0.16.2
```

```python
from interlacing import Chain, run

def spread(samples: list) -> float:
    return max(samples) - min(samples)

def describe(value: float) -> str:
    return f"range = {value:.1f}"

result = run(Chain(spread, describe), {"in": [2.0, 4.5, 3.0]})
print(result.last("out"))  # range = 2.5
assert result.outputs == {"out": ["range = 2.5"]}
```

Three names matter here. A standalone `spread` takes an input called `samples`, its parameter name. `Chain` exposes the ends of the graph as `in` and `out`. One run may emit several messages on an output, so `outputs["out"]` is a list and `last("out")` reads its last value. An **activation** is one execution of a node with a batch of inputs. A **run** can contain many activations.

<a id="graph"></a>

## 2 · Make a report wait for review

Both the reviewer and the writer need the original measurement. The writer also needs a release signal. Explicit edges express these two dependencies. This script runs independently.

```python
from interlacing import Engine, FunctionNode, GraphBuilder, Port

def review(value):
    if value < 0:
        raise ValueError("negative measurement")
    return True

def report(value, approved):
    return f"accepted: {value}"

check = FunctionNode("check", review, inputs=[Port("value", int)],
                     outputs=[Port("ok", bool)])
writer = FunctionNode("writer", report, inputs=[Port("value", int), Port("approved")],
                      outputs=[Port("text", str)])

source = FunctionNode("source", lambda x: x, inputs=[Port("x", int)], outputs=[Port("out", int)])
graph = (GraphBuilder().add(source, check, writer)
         .input("measurement", "source.x")
         .connect("source.out", "check.value")
         .connect("source.out", "writer.value")
         .connect("check.ok", "writer.approved", "signal")
         .output("report", "writer.text").compile())
assert Engine(graph).run({"measurement": 8}).last("report") == "accepted: 8"
```

A `Node` does the work. A `Port` names an input or output and specifies its shape. An `Edge` joins ports. `GraphBuilder.compile()` checks the wiring and produces a `Graph`. The default `Firing.ALL` waits for required inputs. Use `Firing.ANY` when any triggering input can start work. Here `approved` carries the event alone: the payload is `None`. To carry an approval or refusal as a Boolean, use a `value` edge and inspect it downstream. Dependencies determine ordering. Additional workers let ready nodes run concurrently.

**Output contracts.** `Port("score", float, check=validate, repairs=1)` checks the type and applies the relevant protocol and application checks. A validator can return a reason string to tell the sender what went wrong. When repairs are exhausted, the default is failure; `invalid="warn"` records the problem and allows delivery. Use `required=False` for optional inputs and `many=True` for an input fed by several edges. Port checks handle errors detectable at emission. For a judgment that needs other results, see [Challenges](#review).

<a id="edges"></a>

## 3 · Choose an edge protocol

One output may feed several consumers. Each edge determines what the receiver keeps, whether arrival triggers work, and whether the sender waits for a reply. Camera frames, speed settings and actuator commands need different answers. [Runnable example](../examples/graphs/camera_to_arm.py)

| Protocol | Choose it for | Receiver behavior |
|---|---|---|
| `value` | Every simulation result matters | Values queue in order |
| `latest` | Only the latest frame matters | Replaces an unread value; arrival can trigger work |
| `state` | A current setting | Retains the latest value for work triggered by another input |
| `signal` | Completion of a step | Delivers an event with payload `None` |
| `stream` | Incremental chunks | The consumer can read while the sending activation is writing |
| `call` | A model or program needs another node’s reply | The sender waits for the result via `ctx.call` |
| `file` | An existing file in a working directory | Sends a relative path, checked at both ends; use `with_files(DEFAULT, root)` |
| `artifact` | Content addressed by its digest | Stores content and carries a `Ref` in the envelope; use `standard(store)` |

`DEFAULT` includes the first six. Use `FileOf(".png", max_size=5000000)` on a file port to restrict the file. Import `MemoryStore` and `DirectoryStore` from `interlacing.protocols`. To extend the protocol set, implement `Protocol` and register it in `Protocols`. `Delivery` describes delivery behavior and `Envelope` carries a message with its provenance. Feedback loops use explicit back edges with a round limit.

<a id="nodes"></a>

## 4 · Choose a node and execution environment

Methods accept ordinary Python functions and infer ports from their signatures. Use `@node(...)` or construct a `FunctionNode` to customize outputs, checks or inputs. Reach for `Context` and a custom `Node` when you need intermediate messages, challenge handling, checkpoints or device lifecycle hooks. Choose by where the work happens.

| What you have | Entry point and use |
|---|---|
| Local function or stateful Python object | `as_node(fn)` adapts a function. `FunctionNode(..., context=True)` passes the activation context. Subclass `Node` for `open/run/close` and `snapshot/restore` |
| Model | `AgentNode(id, model, ...)` accepts a provider with `chat(system, messages, tools, max_tokens)`. Supply a function as a local tool or use `Tool` with a `call` edge to invoke another node |
| Persistent model conversation | `resident=True` lets one activation repeatedly send and receive. The board holds plans, `wait` listens for messages, and `finish` ends the activation. Hiring requires a limit and templates. [Example](../examples/graphs/resident_team.py) |
| Command, app directory or function needing process isolation | `ProcessNode` wraps a command. `FolderNode` reads the app’s `node.json` and I/O contract. `IsolatedNode` runs a function in a terminable subprocess. [Folder example](../examples/graphs/folder_as_node.py) |
| Remote service retaining state across activations | Serve it with `serve_node(node)` or the `node-serve` command. Put a `RemoteNode` proxy in the graph. Discovery and activation require a reachable server. [Example](../examples/graphs/served_elsewhere.py) |
| Device, destination or person | `SourceNode` emits iterable readings through `out`. `SinkNode` writes each input and can reply with `answers=True`. `HumanNode` delegates a response to a person |
| File and command tools | `Workspace` allocates each node an `Area`. `WorkerNode` equips a model with file and command tools. `ToolPool` loads tools by path. Commands inherit host process permissions. [Example](../examples/graphs/worker_in_workspace.py) |
| Route many named inputs into one or more outputs | A `Desk` combines named ports with a sorting function. [Example](../examples/graphs/desks_between_blocks.py) |

**Test the wiring with a scripted model.** This agent runs on the actual graph engine while a local function supplies the model response. For a live service, import `AnthropicProvider` or `OpenAICompatProvider` from `interlacing.models`. `Provider` defines the interface and `ChatResult` carries replies and tool calls.

```python
from interlacing import AgentNode, ChatResult, FakeProvider, run

def reply(system, messages, tools):
    return ChatResult(text="ready")

agent = AgentNode("reviewer", FakeProvider(reply))
assert run(agent, {"task": "Check availability"}).last("out") == "ready"
```

<a id="compose"></a>

## 5 · Compose, reuse and change a graph

A **method** is a reusable wiring pattern. Use `Chain(a, b)` for sequential work, `Fan(a, b)` to split and join, and `Loop(a, b, max_rounds=3)` for bounded feedback. Choose `Tree` for multilevel coordination, `Star` for a single hub, `Governed` for operator consent, and `Pool` for a capped set of members activated on demand. A method’s `build()` yields a graph for the engine. Implement `Method.build(protocols)` to supply a new pattern. [Methods and port conventions](GRAPH.en.md)

| Requirement | API and lifetime |
|---|---|
| Expose an existing subgraph as a node | `GraphNode` exposes the subgraph’s inputs and outputs |
| Reuse one design with independent state | `Block(factory)` stores a graph recipe and creates a fresh instance per use. Compose with `seq` or `par`. [Example](../examples/graphs/blocks_joined.py) |
| Change the running graph | A node calls `ctx.change(Change().add(...).connect(...))`. The engine validates the whole change before applying it. Journaled runs need a way to rebuild added nodes on resume. [Example](../examples/graphs/changed_while_running.py) |
| Execute a graph just generated by a node | `SpawnNode` accepts a graph description and inputs. A `Catalog` and limits such as maximum node count bound the subgraph. `tool()` exposes this entry to an agent. [Example](../examples/graphs/grown_at_run_time.py) |

<a id="run"></a>

## 6 · Execute, observe and recover

| Entry point | Use it when | What to inspect |
|---|---|---|
| `run(thing, inputs, **options)` | Running a function, node, method or graph in a script or test | Returns a `Result`; inspect `outputs`, `last(name)` and `complete` |
| `Engine(graph, ...).run(inputs)` | You need scheduling, budgets, journaling or observation | `workers` controls parallel worker threads, `time_limit` bounds an activation, `budget` totals reported usage, and `observe` receives events. For async execution use `await engine.arun(inputs)` |
| `Engine(..., journal=Journal(path)).resume()` | Your application owns the graph and journal and needs to resume after a restart | Recreate the engine with the same graph and journal. `Journal(path, sync=True)` additionally syncs every record to disk |
| `interlacing.runs.Run` | Processes hand off a run directory, often while waiting for a person | Use `Run.create(directory, description, ...)`, then `start(inputs)`. Later open `Run(directory)` to inspect, answer or resume |

**Read a run.** `Result.rounds` counts activations by node, `used` totals usage, `problems` records checks or handled issues, and `held` lists nodes paused or stopped when execution ended. On failure inspect `RunError.node`, `used` and `problems`. A node reports cost with `ctx.use(...)` and checks its remaining activation time with `ctx.time_left()`. The `time_left` helper is also available as an agent tool. A timed-out in-process thread may still perform external actions; use a subprocess when termination is required.

**Three places a long task can yield.** (1) `ctx.receive()` waits for another message within the activation. (2) `ctx.checkpoint()` lets executing code respond to pause or stop. (3) `ctx.commit()` journals progress; a stateful custom node describes restorable state with `snapshot/restore`. Unfinished, unsaved work can run again. For external writes, `ctx.once(name, do)` reuses a recorded result. A crash between the external action and its record can still lead to a retry; pass a stable `ctx.key` to an external service that supports idempotency. [Recovery tests](../tests/test_commit.py) · [External action tests](../tests/test_once.py)

<a id="review"></a>

## 7 · Who handles a questionable result?

| What happened | Who decides | Behavior in 0.16.2 |
|---|---|---|
| A produces a structurally invalid output | Output port | `Port("score", check=validate, repairs=1)` can trigger repair, warning or failure according to its settings |
| B1 has evidence that A’s simulation result is wrong | B1 raises the question; A answers | Enable challenges with `connect(..., challenges=N)`. B1 calls `ctx.reject(port, reason)` and waits; specify `which` for multiple senders. A explains or corrects. Ordinary nodes can implement `challenged(ctx, rejection)` |
| A knows which message needs to change | A corrects; the receiver applies its policy | Keep the ID from `sent = ctx.emit(...)`, then use `ctx.correct(sent, value)`. Unread values can be replaced. For read values the policy is `rerun/tell/ignore`. After resume, ordinary value inputs can rerun in place. Older journals and unavailable read history use the notification fallback |
| A wants to pause an affected B2 | The graph author declares management and the runtime enforces it | `builder.manage("a", "b2")` lets A pause it via `ctx.control("b2", "pause")`. Operators use `engine.control` or the CLI |

**Try a challenge between ordinary nodes.** Run the following from the repository root. Two estimators return 50 and 76. The merge node challenges the first sender by name. That sender revises its estimate to 74 and corrects the note emitted alongside it. The final area is 75 and the log node receives revised text. This demonstrates challenge routing and related corrections. The graph author decides how affected nodes assess impact and request pauses.

```sh
python examples/graphs/directed_rejection.py
```

<a id="cli"></a>

## 8 · Run graphs from JSON, the CLI or MCP

Python accepts functions directly; JSON describes nodes and edges using names the host can resolve. Use `dump(graph)` to get a description and `load(description, catalog)` to build it. Read or write files with `interlacing.spec.read/write`. Reloading requires node and function descriptions resolvable through the `Catalog`. Use `interlacing guide --allow MODULE` to look up the format and available functions, or `interlacing.spec.GUIDE`, `functions(catalog)` and `guide(catalog)` from Python. The host chooses capabilities via `--allow`, `--processes`, `--apps`, `--tools` and `--remote`.

**Exercise: resume after a person approves.** From a checkout, install with `python -m pip install -e .` at the repository root. Execute the commands below one at a time with a fresh run directory. The quoting is for a POSIX shell. The first `run` is expected to exit with **3**. Inspect the pending question, record the answer, then explicitly resume. The final output includes `published: A note about tides.`. [Graph file](../examples/data/release.json) · [Functions](../examples/data/steps.py)

```sh
cd examples/data
interlacing check release.json --allow steps
interlacing run release.json --dir demo-run --allow steps --input topic=tides
interlacing status --dir demo-run
interlacing show --dir demo-run
interlacing answer --dir demo-run --json '{"approved": true}'
interlacing resume --dir demo-run
```

For a new run, add `--journal sqlite` to store its execution history in `journal.sqlite3`. The run directory records that selection, so later `resume`, `show`, `status` and `control` use it automatically. Existing runs default to JSONL. Graph definitions, human questions and answers, and workspace files are still kept in the run directory. Python code can select `Run.create(..., journal="sqlite")` or pass `SQLiteJournal(path)` to `Engine`.

Add `--durable` with `--journal sqlite` on `run` or `serve` to commit each delivery before its receiver can start. The runtime records actual input deliveries before node code runs. Recovery loads a periodic checkpoint and the later events. An unfinished node can replay with its original inputs. A matching operation reuses its committed delivery and publishes nothing again. Replay that produces a different value or different graph edit fails. Explicit progress uses `ctx.commit()`, `snapshot/restore` and `ctx.resumed`. Python code opts in with `SQLiteJournal(path, durable=True)`.

For `artifact` edges, configure the Python engine with a content store, such as `standard(DirectoryStore(...))`. The default catalog used by the local CLI and `Run.create` omits this protocol. PostgreSQL workers use the content directory configured for the service.

**View history and fork a run.** The following commands apply to a started durable SQLite run. Event `1` records the initial inputs. `checkpoints` lists up to 16 retained recovery snapshots. `show --at` can still inspect any committed event position, including the graph version, output values and unfinished activations at that position.

```console
$ interlacing checkpoints --dir source-run
$ interlacing show --dir source-run --at 1 --timeline
$ interlacing fork --dir source-run --to variant-run --at 1 --input in=42 --execute
$ interlacing resume --dir variant-run
```

Fork at a position where all activations have settled and choose a new directory. You can replace unconsumed graph inputs and supply inputs omitted at event `1`. Each fork has its own journal, run ID and workspace, with a reference to the parent run and event. Workspace files and human questions and answers are outside the historical snapshot. A fork defaults to inspection only. With `--execute`, `resume` runs nodes with the inherited capabilities, so they can produce external side effects. Python callers can use `Run.checkpoints()`, `Run.show(at=...)` and `Run.fork(..., at=..., inputs=..., execute=True)`.

To try a revised intermediate result, select a settled root event in a static graph. Pass an output edit such as `{"node":"quote","token":"price#1","version":1,"value":120,"reason":"updated offer"}` to `Run.fork(..., output=edit)` or as JSON to `fork --output`. Forward value edges follow their normal correction policies. A new branch can therefore reuse independent work and ask for a fresh approval where the quote was consumed. See the [service examples](POSTGRES.md).

This mode supports value and artifact edges with an optional challenge budget, activation-start input binding, additive graph edits and pause, stop and resume. Choose the standard mode for streams, calls, mid-activation receive, node restart or replacing the graph on resume. For external effects, use provider idempotency keys where available. Durable delivery deduplicates graph messages. An interrupted user function can still run again.

**Correct a recorded message.** Use `ctx.correct(token, value)` with a token emitted by the same node. An unread copy is replaced in its mailbox. When a reader uses `on_correction="rerun"`, its original activation is queued with the revised input and every other original input. It updates its previous output slots as it emits, which can schedule downstream rework. `tell` delivers the revision as new input, and `ignore` leaves the reader's result as it was. Repeating the current value keeps the same version. The journal tracks rework across restarts and checks bound input versions before each publication. A superseded activation's earlier commits remain in the history.

In a historical `show --at` view, `rework` lists the original rounds and delivery IDs awaiting rework. Add `--timeline` to see correction versions, rework bindings and superseded activations. Saved continuations and activations that changed the graph receive revisions as new input when their policy is `rerun`. Rework updates slots that the node emits again. Existing outputs it omits remain recorded. External effects follow the node's own execution and idempotency rules.

Use `interlacing show --dir demo-run` to inspect recorded deliveries by input port. While the run waits for approval, `publish.note` has a delivery from `draft.note` and `publish.verdict` awaits the first recorded delivery from `editor.answer`. The command reads the saved graph and complete journal lines without loading node code. It reports historical evidence even for a running process. Committed additions, removals and wiring changes reconstruct the recorded graph.

Add `--timeline` to inspect activation rounds, challenges and verdicts, switches and usage in journal order. New activation records include `elapsed_seconds`, wall time for that segment including waits. Older records omit it. Failed runs also return `failure`. Export node and port connections with `--format mermaid` or `--format dot`. An external graph replacement recorded only as a digest leaves the last known topology labeled as such. [Two review paths for a simulation](../examples/graphs/simulation_review.py)

With the timeline, `provenance` links each recorded output slot to its original and replacement records. Use the `scope_id` and `event_seq` to locate a record, then follow its reads to the delivery and producer. Completed subgraphs retain this metadata under `children` after their recovery journals are cleaned up. Legacy records can be viewed with an unknown scope. Missing or ambiguous delivery references are marked explicitly.

Exit codes for `run/resume`: `0` completed, `1` failed or refused, `3` waiting for a person, `4` stopped. `status` inspects state and questions, `files` shows file ownership, and `control pause|resume|stop|restart` operates on nodes or their managed subtree. The directory holds `graph.json`, `options.json`, `inputs.json`, the selected journal, `state.json`, questions, answers and files. SQLite may create `-wal` and `-shm` sidecars alongside its journal. Use `resume --dir` to continue a run and `control resume NODE --dir` to switch on a paused node.

**MCP entry point.** Configure the client to launch `interlacing serve --runs /path/to/runs` over stdio, adding the host capabilities it needs. Add `--journal sqlite` to select SQLite for new runs from that server. A typical sequence is `guide → check → start → status`; use `answer → resume` for human input. Other tools are `runs`, `stop` and `control`. For a long job, pass `wait: false` to `start/resume` and query its status. To serve a single remote node, use `node-serve`. [MCP tool parameters](GRAPH.en.md)

**Try the simulation review over MCP.** From `examples/data`, configure the client to launch `python -m interlacing.cli serve --runs /absolute/path/to/runs --allow steps` over stdio. Pass the contents of [cooling_review.json](../examples/data/cooling_review.json) as `start.graph` with inputs `{"steps":100}`. B1 receives B2’s draft, 36.6032. Call `show` with the run ID and `timeline: true` to see that B2 ran and B1 awaits a person. In this example the draft used a percentage as a fraction. Submit `{"units":"percent","reading":36.6032}` with `answer`, then call `resume`. The final `outputs.report` contains only 0.366 and `used.simulations` is 1.

Use the PostgreSQL collaboration service for project memberships, human task assignment and multiple workers. [Operator guide and HTTP API](POSTGRES.md)

<a id="maintain"></a>

## 9 · Maintainer map and troubleshooting

Start with the layer that owns the behavior, then find a test that exercises its observable effects.

| Change you want | Start here | Verify with |
|---|---|---|
| Ports, schemas, wiring rules, activation contract | [base](../src/interlacing/base/) | [Base tests](../tests/test_base.py) |
| Readiness, delivery or recovery | scheduler, wiring, recorder and journal in [engine](../src/interlacing/engine/) | [Engine](../tests/test_engine.py) · [Fault injection](../tests/test_faults.py) |
| Edge encoding, reads or content storage | [protocols](../src/interlacing/protocols/) | [Protocol tests](../tests/test_protocols.py) |
| New node, device adapter or tool wrapper | [nodes](../src/interlacing/nodes/) | [Function adaptation](../tests/test_light.py) · [Remote node](../tests/test_remote.py) |
| Topology or reusable graph composition | [methods](../src/interlacing/methods/) · [blocks](../src/interlacing/blocks/) | [Methods](../tests/test_methods.py) · [Blocks](../tests/test_blocks.py) |
| Model adapter or response parsing | [models](../src/interlacing/models/) | [Providers](../tests/test_providers.py) · [Agent](../tests/test_agent.py) |
| JSON, host capabilities or graph authoring guide | load, catalog and guide in [spec](../src/interlacing/spec/) | [Graph format](../tests/test_spec.py) · [Spawn](../tests/test_spawn.py) |
| Python shortcut, run directory, command or MCP tool | [quick.py](../src/interlacing/quick.py) · [runs.py](../src/interlacing/runs.py) · [cli.py](../src/interlacing/cli.py) · [serve.py](../src/interlacing/serve.py) | [CLI](../tests/test_cli.py) · [MCP](../tests/test_serve.py) |

| Symptom | First thing to check |
|---|---|
| Graph or port error at compile time | Read the node and port in `GraphError`. Check endpoint names, protocol and back edges. For JSON run `check` and inspect `SpecError` for resolution failures |
| A node stays idle | Check required inputs, updates arriving only on `state`, paused nodes and pending human questions |
| Missing or surprising output | Inspect the full `outputs` lists, `rounds`, `problems` and `complete`. Look for a correction, round limit or rejected output |
| External simulation repeats after recovery | Locate the external action relative to the journal write. Can the service look up the same task by `ctx.key`? |
| Feature works in Python but fails in JSON | Check spec loading and dumping, host capabilities in the Catalog, and CLI/MCP options. Exercise a round trip or an end-to-end path |

### Public names at the package root

All names below are available directly from `interlacing`. For the installed signature, import the package and use e.g. `help(interlacing.Port)`. See the [design reference](GRAPH.en.md) for detailed parameters, graph formats and limits. Groups below follow the package exports.

| Module | Names |
|---|---|
| `base` | `Node`, `Port`, `Firing`, `Context`, `Schema`, `Edge`, `Invalid`, `Protocol`, `Protocols`, `Delivery`, `Envelope`, `Ref`, `Graph`, `GraphBuilder`, `Change`, `GraphError`, `NodeError`, `Method` |
| `engine` | `Engine`, `Result`, `RunError`, `OverBudget`, `Journal`, `SQLiteJournal`, `Transport` |
| `nodes` | `FunctionNode`, `AgentNode`, `Tool`, `ProcessNode`, `SourceNode`, `SinkNode`, `HumanNode`, `GraphNode`, `Desk`, `IsolatedNode`, `WorkerNode`, `FolderNode`, `RemoteNode`, `serve_node`, `time_left`, `Workspace`, `Area`, `ToolPool`, `node`, `as_node` |
| `methods` | `Chain`, `Loop`, `Fan`, `Tree`, `Star`, `Governed`, `Pool` |
| `blocks` | `Block`, `seq`, `par` |
| `protocols` | `DEFAULT`, `standard`, `File`, `FileOf`, `with_files` |
| `models` | `Provider`, `FakeProvider`, `ChatResult` |
| `quick` | `run` |
| `spec` | `Catalog`, `SpecError`, `SpawnNode`, `dump`, `load` |

`Schema` describes accepted values. `Invalid` reports an output rejected during emission; `NodeError` covers node errors. `OverBudget` signals an exceeded usage limit. `Transport` is the hook for moving envelope bodies. `File` implements the path protocol and `FileOf` restricts accepted files. `__version__` identifies the installed release.
