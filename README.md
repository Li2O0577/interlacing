# interlacing

<p align="center"><img src="https://raw.githubusercontent.com/Li2O0577/interlacing/main/docs/images/interlacing.jpg" alt="interlacing" width="720"></p>

**English** · [中文](README.zh-CN.md)

Join anything into a graph and run it.

[Python user and maintainer guide](docs/HANDBOOK.en.md)

- A **node** is anything that takes information in and gives information out: a model, a function, an external program, a device, a person, or another graph.
- An **edge** carries information from one node to another under a written protocol: queued values, latest value only, state, signals, streams, calls that are answered, files passed by path.
- A **method** is a ready-made way of joining nodes: chain, loop, fan-out and join, tree, star, a tree that needs a person's consent, a pool of members that appear when called.

The engine runs the graph and looks after the run: outputs are checked, usage is counted against a budget, activations are timed, everything is journaled so an interrupted run carries on, and an observer is told what happens as it happens. The package is a kernel to build frameworks on. The core runs on the Python standard library. The optional PostgreSQL collaboration service also requires the postgres extra (psycopg).

## Install

```sh
pip install interlacing              # published release
pip install -e .                     # core from this checkout
pip install -e ".[postgres]"         # this checkout with the collaboration service
```

Python 3.11 or later, on Linux, macOS and Windows.

## A first graph

```python
from interlacing import Chain, run

def double(x: int) -> int: return x * 2
def show(x: int) -> str: return f"={x}"

print(run(Chain(double, show), {"in": 21}).last("out"))     # =42
```

A plain function is a node: its name is the node's id, its parameters are the inputs, its annotations the
schemas, and what it returns leaves through `out`. Every method takes functions, nodes and other methods
alike. What a node is can be as small as one function or as large as a whole application in a folder; the
graph only draws the parts that need to be kept apart.

The same graph written out in full, where every name and protocol is chosen by hand:

```python
from interlacing import DEFAULT, Chain, Engine, FunctionNode, Port

double = FunctionNode("double", lambda x: x * 2, inputs=[Port("x", int)], outputs=[Port("y", int)])
show = FunctionNode("show", lambda x: f"={x}", inputs=[Port("x", int)], outputs=[Port("y", str)])

graph = Chain(double, show).build(DEFAULT)
print(Engine(graph, DEFAULT).run({"in": 21}).last("out"))     # =42
```

`@node` sits between the two: it writes only what differs from what the function says about itself.

```python
@node(outputs=["kept", "dropped"], check=...)
def sort(x: list): ...
```

## What it does

**A model with tools.** A tool is a call edge to an ordinary node, so a dictionary, a program and a robot arm are all answered the same way.

```python
from interlacing import AgentNode, GraphBuilder, SinkNode, Tool

agent = AgentNode("agent", model, system="...", tools=[Tool("lookup", "Look a word up"), Tool("move", "Move the arm")])
graph = (GraphBuilder().add(agent, dictionary, SinkNode("arm", arm.move, answers=True))
         .connect("agent.lookup", "dictionary.x", "call")
         .connect("agent.move", "arm.x", "call")
         .input("in", "agent.task").output("out", "agent.out").compile(DEFAULT))
```

`model` is any object with `chat(system, messages, tools, max_tokens)`. `interlacing.models` has an Anthropic interface, an OpenAI-compatible interface, and `FakeProvider` for tests.

**Outputs that are checked.** A port says what may leave through it and what happens when something else does: the node fails, the payload goes on with a warning, or the node is told what is wrong and may send again.

```python
Port("plan", dict, check=valid_plan, repairs=2)      # a model is told why its plan was refused, twice at most
```

**Files passed by path.** Nodes send relative paths, not contents. The file is checked when it is sent and again when it arrives.

```python
protocols = with_files(DEFAULT, "work")
Port("chart", FileOf(".png", max_size=5_000_000), repairs=2)
builder.connect("draw.chart", "report.chart", "file")
```

**A workspace laid out by node.** A run's files live in one workspace. Each node has its own directory, `shared/` is for everyone, and a block is one more layer (`review/writer/`, `review/shared/`). A node reads everything and writes to its own directory and the shared ones of its layers, unless its description says otherwise (`"reads"`, `"writes"`). The workspace records which node wrote each file in which activation: `interlacing files --dir RUN`.

**A whole application as one node.** A folder that holds a `node.json` (how to start it, what it takes, what it gives) is a node, whatever language it is written in and however large it is. Each activation starts it in its folder, hands it the inputs as one JSON object and takes one back; a plain command-line program that reads and writes text fits too. A graph file names a folder with `{"kind": "folder", "path": "solver"}`, and only inside the directory its starter gives (`--apps DIRECTORY`).

```python
solver = FolderNode("apps/solver")      # apps/solver/node.json: {"command": ["python", "main.py"], "inputs": ["problem"], "outputs": ["answer"]}
```

**A node that lives elsewhere.** A node that should stay up between activations (an open device, a loaded model) is served where it lives, in another process or on another machine, and `RemoteNode` stands for it in the graph. One JSON object per line goes over TCP; emits, what the node uses and a stop come through. A server listening beyond its own machine needs a token, read from an environment variable; a graph file reaches only the addresses its starter allows (`--remote ADDRESS`).

```python
serve_node(arm)                                       # or, on the other machine: interlacing node-serve mypkg.arm:arm --listen 0.0.0.0:7000 --token-env ARM_TOKEN
far = RemoteNode("arm", "tcp://lab-pc:7000", token=os.environ["ARM_TOKEN"])
```

**A worker that works freely in its area.** `WorkerNode` is an agent with tools for the files of its area (list, read, write, edit, delete, move) and a tool that runs a command in its own directory. Its inputs and outputs are an agent's, checked the same way.

```python
worker = WorkerNode("solver", model, Workspace("work").area("solver"), outputs=[Port("report", FileOf(".pdf"))])
```

**A tool pool.** A directory of tool files: a Python file whose function's docstring and parameters say what the tool is, or a JSON file that names any program. An agent is given tools by path (`"tools": ["plot.py", "charts"]`). Each use runs in a process of its own, in the node's directory, and is ended when it overruns. Which directory is the pool is set by whoever starts the run (`--tools DIRECTORY`), not by the graph.

```python
agent = AgentNode("analyst", model, tools=ToolPool("tools").tools("charts"))
```

**Designs used as units.** A graph that has been worked out becomes a block, used many times and joined with others.

```python
review = Block(lambda: Loop(writer(), critic(), max_rounds=3), name="review")
study = seq(clean, par(review, review, review), publish)       # in a row, side by side, nested
graph = Chain(intake, study("study"), report).build(DEFAULT)
```

**Desks at the doors of a block.** A `Desk` takes in what arrives from several places, sorts it, and hands it out to several places. At the way into a block it shares the work among the members; at the way out it makes one answer of what they gave. Where the sorting needs judgement, an agent with the same inputs and outputs stands in the same place.

```python
team = Fan(analyst, modeller,                                   # each member gets its own part, by name
           split=Desk("door", takes=["brief", "data"], gives=["analyst", "modeller"], sort=share),
           join=Desk("exit", takes=["analyst", "modeller"], sort=report))
```

**Graphs that are not fixed in advance.** A node changes the graph it runs in with one explicit operation, `ctx.change(Change().add(...).connect(...))`: the change is checked by every rule a graph is built by, refused whole or made at once, journaled with the activation that asked for it, and made again when the run is taken up. `Pool` keeps places for members that do nothing until they are called for. `SpawnNode` builds and runs a graph that a node wrote during the run, within limits set by whoever made the node.

```python
grow = SpawnNode("grow", Catalog(allow=["my_steps"]), max_nodes=20)
graph = Chain(planner, grow).build(DEFAULT)                    # planner sends {"graph": ..., "inputs": ...}
designer = AgentNode("designer", model, tools=[grow.tool()])   # or as a tool that tells the model the format
```

**Runs that are looked after.**

```python
engine = Engine(graph, DEFAULT, workers=4,
                journal=Journal("run.jsonl"),                  # an interrupted run is taken up with resume()
                time_limit=600,                                # the longest one activation may take
                budget={"tokens": 2_000_000},                  # nodes report what they use; over it, the run stops
                observe=print)                                 # every event, as it happens
```

**A switch on every node.** Any node, or several at once, can be paused, resumed, stopped or restarted while the run goes on: by the operator (`engine.control`, `interlacing control`, the MCP tool `control`), or by the node that manages it. Who manages whom is written in the graph (`manage("lead", "a", "b")`), and a node can only switch those under it. A running node stops at its next step (an agent between two calls to its model) and, when paused, waits there with its place and its conversation kept. Every switch is in the journal. It reaches beyond the process: a run going in another process is switched through its run directory, a `RemoteNode` makes its far side wait at its own next step, and a program a node runs is suspended with it (on Linux and macOS), its own time limit waiting too.

**Agents that stay.** `AgentNode(..., resident=True)` is one activation from start to finish, ended when the agent says it has finished. It sends each output when it is ready, waits for the others' messages (`wait`, which holds no worker and says so when nothing more can come), keeps a task board (`plan`, `done`, `note`) and saves its progress, so a run taken up again goes on where it stood. Underneath are two things any node can use: `ctx.receive()`, to take the next message in the middle of an activation, and `ctx.commit()`, to save progress. Plain functions are tools too: `AgentNode(tools=[check, time_left])`, called by the agent itself, with no node or edge.

**Taking something back.** `ctx.emit` returns the message's id (`"out#3"`), and `ctx.correct(id, new)` puts something else in its place. Where the message has not been read yet, it is simply replaced. Where it has, each receiver does what its `on_correction` says: a program runs again with the corrected value and corrects what it sent in turn (`rerun`, the default), an agent is told and decides (`tell`, an agent's default), or nothing happens (`ignore`). A graph output is put right in place. Resident agents get a `correct` tool.

**Doubting what comes.** An edge may take rejections: `connect(a, b, challenges=2)`. Its receiver judges what came, only yes or no, and when it is not what it expected calls `ctx.reject(port, reason)` and waits until the sender explains (the value stands) or corrects it (the correction is its answer). The edge takes that many rejections in all. An agent receiving on such an edge is told, by a default prompt it can replace, to judge first, and has a `reject` tool; an agent whose message is rejected answers with `explain` or `correct`, called back when it has finished. A program answers that its result stands, unless it says otherwise in `challenged`. On a port fed by several edges, `ctx.reject(port, reason, which)` names the one it rejects (by its sender); a rejection lists what else the same activation sent, so the sender can correct that too.

**Agents that build their own team.** Off by default: `AgentNode(..., resident=True, hire=3, templates={"researcher": make})` may hire three members in the whole run, only from its templates. Each member becomes a node it manages, with a line to it (`send_to_<name>`) and one back (`from_<name>`); the agent also gets `dismiss`, `control`, `team` and `connect` (within its team only). A member that may hire gets no more than its hirer, and works in a directory inside its hirer's. A member that fails does not fail the run: its hirer is told, and restarts it, dismisses it or gives up (`fail`); any manager can do the same with `oversees = True` and `ctx.failures()`. Underneath is `ctx.change` with two new operations, `ports` and `manage`; nodes of a graph run inside a node are switched by path (`"box/inner"`).

```python
graph = GraphBuilder().add(lead, a, b).manage("lead", "a", "b")...      # in lead: ctx.control("a", "pause")
engine.control(["a", "b"], "restart")                                  # from outside: a and b start their work again
```

A node that must not do something twice outside the graph wraps it in `ctx.once`: the result is kept in the journal, and when the activation runs again it is returned without being done again.

```python
receipt = ctx.once("charge", lambda: bank.charge(order, key=ctx.key))   # ctx.key names this activation, the same on every attempt
```

**Nodes that are coroutines, and runs that are awaited.** A node's `run` (or the function given to `FunctionNode` and `@node`) may be `async def`. The engine then runs on an event loop: those nodes are awaited there without a thread each, so a hundred agents waiting on a model take `workers=100` and no extra threads; the other nodes still go to threads. One that overruns its time is cancelled. A call from such a node is `await ctx.acall(port, payload)`. Where the caller is a coroutine, `arun` runs the graph on the caller's loop; cancelling the wait stops the run.

```python
result = await Engine(graph, DEFAULT).arun({"in": 21})          # also aresume() and ago()
```

**A function that can be ended.** A thread cannot be stopped from outside, so a function that overruns in the engine's own process is abandoned. `IsolatedNode` runs a function in a process of its own and kills it when it overruns or the run stops; a crash in it fails that node and nothing else.

```python
solve = IsolatedNode("solve", "mypkg.solver.solve", inputs=[Port("problem")], outputs=[Port("answer")], timeout=300)
```

**Graphs written as data.** A graph file holds the structure and whatever differs from the defaults; a prompt is a file that is referred to.

```json
{
  "defaults": {"agent": {"model": "anthropic:some-model", "time_limit": 600}},
  "method": "pool", "size": 10, "max_rounds": 3,
  "root": {"id": "lead", "kind": "agent", "system": {"file": "prompts/lead.md"}, "firing": "any",
           "inputs": ["task", {"name": "reply", "required": false}], "outputs": ["out", "send"]},
  "member": {"kind": "agent", "system": {"file": "prompts/scout.md"}}
}
```

```sh
cd examples/data
interlacing guide  --allow steps                         # how a graph is written, and the functions it may name
interlacing check  release.json --allow steps
interlacing run    release.json --dir /tmp/run --allow steps --input topic=tides    # waiting for a person, exit code 3
interlacing answer --dir /tmp/run --json '{"approved": true}'
interlacing resume --dir /tmp/run                                                   # done, exit code 0
```

A run is a directory. A person in the graph who has not answered yet does not keep a process waiting: the command exits, and the run goes on once the answer has been given. One process at a time runs a directory.

**As an MCP server.** The same operations can be offered to a model in another application. What its graphs may reach, how much they may use and how long a node may take are set on the command line by whoever starts the server, and the client cannot change them.

```json
{"mcpServers": {"interlacing": {"command": "interlacing",
  "args": ["serve", "--runs", "/path/to/runs", "--allow", "mypkg", "--budget", "tokens=500000"]}}}
```

A client reads `guide` first: the format, and the functions the server lets a graph name. Tool calls are answered side by side. `start` and `resume` take `wait: false` to return at once; the run is then asked about with `status` and ended early with `stop`.

Model keys are read from the environment (`ANTHROPIC_API_KEY`, `OPENAI_API_KEY` and their `_BASE_URL`), and are never written to a graph file, a run directory or a journal.

## What it is for

Three worked cases are in [docs/CASES.md](docs/CASES.md) (in Chinese). In short:

| Case | How it is built |
|---|---|
| A physical agent: a multimodal model plans gripper poses as points in space, a controller executes, and the next observation decides what follows | `Loop(observe, planning, act)`; the model's measuring tools are nodes on call edges; the action contract is a check on the plan port, so a bad plan goes back to the model with the reason and never reaches the controller; plans and seconds are budgets. Runnable with stand-ins: [examples/graphs/planner_and_arm.py](examples/graphs/planner_and_arm.py) |
| A modelling assistant: a lead, one owner per question, a solver and a critic who argue until the answer is proved, a person who may step in anywhere | each question is a `Block` holding a `Loop(solve, critic)`; "no proof, not done" is a port check; questions run in `seq`; the person is the operator of a `Governed` tree; the run directory survives restarts |
| A mathematician: a lead sends out explorers in parallel, each hands its work to a proof checker and reports back when it passes | `Pool(lead, explorer, size=10)` with `workers`; each explorer is a `Loop(prove, check)`; three directions given, three explorers exist |

The kernel carries information and looks after the run. How a model thinks, how an arm moves and how a paper is typeset are the business of the nodes.

## Layout

| Layer | Package | Main names |
|---|---|---|
| Abstractions | `interlacing.base` | `Node`, `Port`, `Schema`, `Edge`, `Protocol`, `Envelope`, `Graph`, `GraphBuilder`, `Method`, `Context` |
| Engine | `interlacing.engine` | `Engine`, `Result`, `RunError`, `Journal`, `Transport` |
| Parts | `interlacing.nodes`, `.protocols`, `.methods`, `.models` | `FunctionNode`, `node`, `as_node`, `AgentNode`, `ProcessNode`, `FolderNode`, `SourceNode`, `SinkNode`, `HumanNode`, `GraphNode`, `IsolatedNode`, `WorkerNode`, `RemoteNode`, `serve_node`, `Desk`; `Workspace`, `ToolPool`; `Chain`, `Loop`, `Fan`, `Tree`, `Star`, `Governed`, `Pool` |
| Blocks | `interlacing.blocks` | `Block`, `seq`, `par` |
| Graphs as data | `interlacing.spec` | `dump`, `load`, `read`, `write`, `Catalog`, `SpawnNode`, `GUIDE`, `guide`, `functions` |
| Outside | `interlacing.runs`, `interlacing.cli`, `interlacing.serve`, `interlacing.quick` | `Run`; the command `interlacing`; the MCP server; `run` |

Each layer uses only the layers below it. The engine knows no particular node, protocol or method; a test in `tests/test_engine.py` holds it to that. Inside, the engine is a few small parts, each with one job: the mailboxes a graph becomes (`wiring`), when a node may run (`scheduler`), what is checked and what follows a problem (`checks`), the budget (`ledger`), the journal (`recorder`), time limits (`clock`).

The full description is [docs/GRAPH.en.md](docs/GRAPH.en.md) (also [in Chinese](docs/GRAPH.md)). Twenty examples that run as they stand are in [examples/graphs/](examples/graphs), and graphs written as JSON in [examples/data/](examples/data).

A manager can fork an approved quote, change its value and send it back to the affected reviewer while retaining the original approval. The PostgreSQL service also streams root run progress and supports bounded retries for transient failures. See the [service guide and examples](docs/POSTGRES.md). The [capability review against LangGraph (Chinese)](docs/LANGGRAPH_COMPARISON.md) records the tested scheduling difference and the limits of the service API.

For the matching source and guides of a published version, open its tag on the [GitHub releases page](https://github.com/Li2O0577/interlacing/releases). The [branch review](docs/BRANCH_REVIEW.md) records reproduced issues and recovery checks. The [implementation plan](docs/NEXT_PLAN.md) covers the delivered features and the remaining deployment checks. Both documents are in Chinese.

## Tests

```sh
python -m pip install -e . pytest
python -m pytest -q tests
python scripts/bench_engine.py       # what the engine adds per envelope (about 11 microseconds)
```

The tests run on Linux, macOS and Windows, with scripted models, a stand-in model service and stand-in devices. They include runs broken at random places, with the journal cut short as a dying process leaves it, which must end as unbroken runs do, and a run through the server by the reference MCP client (the `mcp` package, where it is installed). The package passes `mypy`.

## Limits

- Taking a run up again is at-least-once: an interrupted node runs again. `ctx.once` keeps what was done outside the graph from being done again, except in the moment between the deed and its record; for that, give the outside service `ctx.key`.
- Edges are in-process. A node can live elsewhere (`RemoteNode`), but streams and calls between nodes do not cross processes; `Transport` is the place for that, and nothing is there yet.
- Switches and managers hold within one graph: operators can address nodes in an active nested graph by path, for example `box/inner`. On Windows a paused program keeps running; only its node waits.
- Both model interfaces are tested against a stand-in service: tools, pictures, thinking, streams, and waiting when the service is busy. Against a real service they have been tried once, with very small requests; thinking and picture input have not. Device nodes have only met stand-ins.
- A node that overruns in the engine's own process is abandoned, not killed. `IsolatedNode` and `ProcessNode` are killed.
- `allow` is a list of modules whose functions a graph may name. It is not a sandbox, and neither is `IsolatedNode`: its process has the rights of the one that started it. The same holds for a worker's commands and a pool's tools: the file tools stay in a node's area, a command does not have to.
- The MCP server speaks over stdio only. The reference client exercises it in tests, and MCP Inspector has completed the simulation review.

## License

MIT
