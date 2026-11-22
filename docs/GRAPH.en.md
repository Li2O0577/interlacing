# Nodes, edges, methods

**English** · [中文](GRAPH.md)

This package joins anything into a graph and runs it. A node of the graph can be a model, a function, an external program, a device, a person, or another graph; an edge is a written protocol for passing things on; a method is a ready-made structure for joining nodes.

```python
from interlacing.base import Port
from interlacing.engine import Engine
from interlacing.methods import Chain
from interlacing.nodes import FunctionNode
from interlacing.protocols import DEFAULT

double = FunctionNode("double", lambda x: x * 2, inputs=[Port("x", int)], outputs=[Port("y", int)])
show = FunctionNode("show", lambda x: f"={x}", inputs=[Port("x", int)], outputs=[Port("y", str)])

graph = Chain(double, show).build(DEFAULT)
print(Engine(graph, DEFAULT).run({"in": 21}).last("out"))     # =42
```

## Layers

| Layer | Package | Contents |
|---|---|---|
| Abstractions | `interlacing.base` | `Node`, `Port`, `Schema`, `Edge`, `Protocol`, `Envelope`, `Graph`, `GraphBuilder`, `Method` |
| Engine | `interlacing.engine` | `Engine`, `Result`, `Journal`, `Transport`, `Stream` |
| Ready-made parts | `interlacing.nodes`, `interlacing.protocols`, `interlacing.methods`, `interlacing.models` | see below |
| Blocks | `interlacing.blocks` | `Block`, `seq`, `par` |
| Data format | `interlacing.spec` | `dump`, `load`, `read`, `write`, `Catalog` |
| Interfaces | `interlacing.runs`, `interlacing.cli`, `interlacing.serve` | `Run`; the `interlacing` command; the MCP server |

The engine knows only the abstraction layer, never a particular node, protocol or method. A test in `tests/test_engine.py` checks the direction of imports between the layers.

## Nodes

A node declares its input and output ports and implements `run(ctx, inputs)`. It does not know which graph it is in, who is upstream or downstream, or how its outputs travel; everything outside it goes through `ctx`:

- `ctx.emit(port, payload)`: send out of an output port, as many times as it likes in one activation;
- `ctx.call(port, payload)`: send a request out of a call port and wait for the answer;
- `ctx.stopping()`: the run is stopping, and a long activation should return soon.

`run` may also simply return `{output port: payload}`. `open`, `close`, `snapshot` and `restore` are optional.

Ports: `Port(name, schema, required=True, many=False, call=False)`. `schema` describes the shape of what passes: both ends are checked for compatibility when the graph is built, and every payload is checked as the run goes. An output port that declares no shape (anything may come out) may be joined to any input port: building lets it through, and what arrives is checked against the receiving port's shape, failing or warning as that port's `invalid` says. When both ends declare shapes that do not fit (`int` into `str`), building still refuses. An input port with `many` may take several edges and receives a list. An output port with `call=True` makes calls.

Firing: `Firing.ALL` (the default) runs once when every required input has come; `Firing.ANY` runs for each input as it comes.

### Shorter ways to write

A node may be as small as a function or as large as a whole application in a folder; the graph draws only the pieces that need to be managed apart (to run in parallel, to change models, for a person to step in, to carry on after an interruption), and the rest is ordinary code inside a node. So that putting something into a graph is not a burden of its own, the same kernel has three ways of writing, all ending in the same kind of node and the same engine; you can move from the light one to the full one at any time:

1. **A plain function.** Its name is the node id, its parameters the input ports (one with a default is optional), its annotations the port shapes (`int`, `str`, `list[float]`, `dict`; `float` also takes integers; unions and the like are not checked by shape), and what it returns leaves through `out`; annotated `-> None`, it has no output. When its first parameter is called `ctx`, it gets the context. Methods such as `Chain`, `Loop`, `Fan` and `Tree` take functions directly, and other methods too (which become a `GraphNode` named after the method); a function that appears twice is called `name_2` the second time. `as_node(f)` makes a function a node; a lambda has no name and needs an id: `as_node(f, "name")`.
2. **`@node`.** Written on the function definition, it gives only what differs from the defaults: `@node(outputs=["kept", "dropped"], check=..., repairs=2, firing=Firing.ANY, id=...)`. The decorated function can still be called as a function.
3. **In full.** `Node` subclasses, `Port`, `GraphBuilder`, as before.

Protocols may be left out: `build()`, `compile()`, `Engine(graph)`, `GraphNode(id, graph)` and `ctx.engine(graph)` use `DEFAULT` when given none; a graph remembers the protocols it was built with and the engine uses them by default; `connect(a, b)` without a protocol is `value`. `interlacing.run(thing, inputs, **engine arguments)` builds and runs in one step, where the thing is a graph, a method, a node or a function; for a single node or function, the graph's inputs and outputs are its port names.

```python
from interlacing import Chain, Fan, node, run

def clean(text: str) -> str: return " ".join(text.split())
def words(text: str) -> int: return len(text.split())
def letters(text: str) -> int: return sum(c.isalpha() for c in text)

@node(check=lambda line: not line.startswith("0 ") or "no words")
def report(x: list) -> str: return f"{x[0]} words, {x[1]} letters"

run(Chain(clean, Fan(words, letters), report), {"in": " the cat "}).last("out")
```

### Ready-made nodes

| Node | What it is |
|---|---|
| `FunctionNode` | A Python function (`FunctionNode.of(f)`, `as_node(f)` and `@node` read the ports from the function itself) |
| `FolderNode` | A whole application in a folder; see "A whole folder as one node" below. In a JSON graph, `"kind": "folder"` |
| `AgentNode` | A model. Its tools are call ports, or plain functions (see below); with several output ports, the model chooses with `send_<port>`. The model is any object with a `chat` method; `interlacing.models` has ready ones. `memory` may be a function, see below. With `resident=True` it is a resident agent; see "Resident agents" |
| `ProcessNode` | An external program: inputs go to its standard input, its standard output is the output |
| `IsolatedNode` | A Python function run in a process of its own: when it goes past the run's time limit or its own `timeout`, or the run is stopped, the process is killed, and a crash fails only this node. The function must be at the top level of an importable module (or its dotted name); inputs and the return value cross between the processes as JSON, and it gets no `ctx`. It is not a sandbox. In a JSON graph, `"kind": "isolated"`, and the catalog must allow starting processes |
| `WorkerNode` | An agent that works freely in a given workspace: it has file tools and a tool to run commands, with inputs and outputs as usual. See "Workspaces, workers and tool pools" below. In a JSON graph, `"kind": "worker"` |
| `SourceNode`, `SinkNode` | Where outside readings come from and where results go (devices such as cameras and arms are attached here) |
| `HumanNode` | A person. The request goes to `ask(request)`; what it returns is the person's answer |
| `GraphNode` | A whole graph as one node; methods nest through it, and every use of a block is one |
| `SpawnNode` | Receives a graph written as data while the run goes on, builds it, runs it and sends back the result (in `interlacing.spec`; see "Growing while running") |
| `Pass`, `Collect`, `Branch`, `Until` | Only steer the flow: pass on as it came, gather into a list, choose one of two, decide whether a loop goes on |
| `Desk` | A desk: `takes` are the places it receives from, `gives` the places it hands out to, and `sort(received)` turns what came into a mapping from place to payload (without `sort`, every place gets everything); with `wait=False` it handles each thing as it comes. At a block's entrance it is a receptionist, at its exit a dispatcher; where judgement is needed, use an `AgentNode` with the same inputs and outputs |
| `Roster` | Keeps a fixed number of places and hands them out by name; `Pool` uses it |

`AgentNode` leaves two openings, both inactive by default:

- `memory=function`: before each request to the model, what `memory(messages)` returns becomes the new conversation. Compression, summaries, keeping only the last few turns: all of that is written in this function, and the kernel prescribes none of it.
- Pictures: wherever `{"mime": "image/png", "data": <base64>}` appears in what arrives, it goes to the model as a picture, with an `[image 1]` placeholder left in the text.

- Plain functions as tools: `AgentNode(..., tools=[check, time_left])`. The function name is the tool name, the docstring its description, and the parameters and annotations make the JSON schema of its arguments; parameters without defaults are required. A parameter named `ctx` is not filled by the model but given the node's context. Such a tool is called by the agent itself, with no node or edge in the graph. `Tool.of(function, name=None, description=None)` is the explicit form; a graph file writes `{"function": "module.name"}`, subject to `allow`. The built-in `time_left` tells the agent how many seconds are left before its time limit. Timed reminders, checking programs and the like are a few lines of function handed to the agent; not every program has to be a node of the graph.

The system prompt is empty by default: the package writes no prompts for you. To limit which files an agent reads and writes, or what directory layout it keeps, write it in its `system`; for a hard limit, make reading and writing files a tool node that checks the paths.

### A whole folder as one node

Put a `node.json` in a folder, saying how to start the application, what it takes and what it gives, and the folder is a node. The application may be written in any language and be of any size, and need not know about interlacing:

```json
{
  "command": ["python", "main.py"],
  "inputs": ["problem", {"name": "data", "required": false, "type": "list"}],
  "outputs": ["answer", "notes"],
  "timeout": 600
}
```

- Each activation starts `command` in the folder. A command beginning with `python` uses the Python running the graph.
- The inputs that came are written to standard input as one JSON object (input name to value), and also to the file named by the environment variable `INTERLACING_IN`.
- The application's answer is a JSON object (output name to value): written to the file named by `INTERLACING_OUT`, so that standard output can print anything; without that file, the whole of standard output is the answer. An output port that does not appear sends nothing; an output name it does not know is a failure.
- `"format": "text"`: at most one input and one output, read and written as plain text; suited to ordinary command-line programs.
- A port's `type` may be `any` (the default), `int`, `float`, `number`, `str`, `bool`, `list` or `dict`.
- A non-zero exit is a failure, reported with the end of standard error; past `timeout` or the run's time limit, or when the run is stopped, the process is killed. When the graph has a workspace, `INTERLACING_WORKSPACE` is its path.
- It is not a sandbox: the application has the rights of the process that starts it.

In Python: `FolderNode("apps/solver")`; the id defaults to the folder name. In a JSON graph: `{"id": "solve", "kind": "folder", "path": "solver"}`. The path is relative to the application directory given by whoever starts the run (`Catalog(apps=directory)`; `--apps directory` for the command line and the MCP server), anything outside it is refused, and, as for `process`, starting processes must be allowed. The content of `node.json` goes into the graph digest; change it, and an older run carries on with `resume(change=True)`.

### Nodes in another process or on another machine

`FolderNode` starts the program again for each activation; some things should stay open and keep what they hold across activations: an open device, a loaded model, a connection. Such a node is "served" in the process where it lives, and `RemoteNode` stands for it in the graph:

```python
server = serve_node(arm)                     # far side: any node or plain function; or interlacing node-serve
far = RemoteNode("arm", server.address)      # in the graph: ports are asked of the far side, each activation runs there
run(Chain(plan, far), {"in": task})
```

- On another machine: `interlacing node-serve mypkg.arm:make --listen 0.0.0.0:7000 --token-env ARM_TOKEN`. The target is written `module:name` and is a node, a plain function, or, with `--make`, a function that makes the node. It first prints one line `{"address": ..., "node": ...}`, then serves until interrupted.
- On the connection, one JSON object per line (UTF-8): after `hello` (with the token), each activation sends a `run` (with the inputs, the round and `ctx.key`), and the far side answers with `emit` and `use`, then `done` or `error`; when the run is to stop, the near side sends `stop` and the far side's `ctx.stopping()` becomes true.
- Only plain data goes over and comes back. The far side runs one activation at a time, whoever asks; `ctx.call` is refused (the far side cannot reach into the graph), and `ctx.once` does its work every time, since the far side has no journal. Usage on the far side comes back to the run through `use` and counts against the budget.
- If the connection breaks in the middle of an activation, the activation fails and can be retried as usual or taken up from the journal; the next activation connects again.
- A server not listening on this machine (`127.0.0.1`) must have a token, which every client must give. This is not encryption: across machines, keep it on a network already trusted, or go through a tunnel.
- In a JSON graph: `{"id": "arm", "kind": "remote", "address": "tcp://host:7000", "token_env": "ARM_TOKEN"}`. A graph file may connect only to addresses allowed by whoever starts the run (`Catalog(remotes=[...])`; `--remote address`, repeatable, for the command line and the MCP server); the token is read only from the environment variable `token_env` names, never written in the graph file, the run directory or the journal. When ports are not written, they are asked of the far side when the graph is built, so the far side must already be serving then.

## Edges and protocols

An edge goes from one output port to one input port and carries a protocol name. The protocol decides what may pass, how it is delivered, and whether its arrival starts the receiving node.

| Protocol | Delivery |
|---|---|
| `value` | One envelope per payload, queued in order; one activation takes one |
| `latest` | Only the newest is kept; an older unread one is replaced |
| `state` | The newest value can be read at any time, but its arrival does not start the node and reading does not consume it |
| `signal` | No payload, only the fact of arrival |
| `stream` | What the sender emits in one activation is a sequence; the receiver reads while it is written |
| `call` | Request and answer: the sender waits, the receiving node runs for this request, and its return value is the answer |
| `artifact` | The payload goes into the content store; the envelope only holds a reference (hash, size, type) (`standard(store)`) |
| `file` | The edge carries a file's relative path; the file itself stays where it is (`with_files(DEFAULT, working_dir)`, see "Passing files by path") |

`DEFAULT` is the registry of the first six; `standard(store)` adds `artifact`. Your own protocols subclass `Protocol`, implement `check`, `encode` and `decode`, and are registered in `Protocols`.

Back edges: `connect(..., back=True, max_rounds=N)`. With back edges removed the graph must be acyclic, every back edge must really close a cycle, and each one has a round limit, so no graph runs forever.

## Output checks

Every payload a node emits is checked three times at the moment it is emitted: the port's `schema` (is the shape right), the protocol of each edge (for example, the `file` protocol checks that the file exists), and finally the check functions attached to the port:

```python
def positive(value):
    return None if value > 0 else f"{value} is not positive"   # a string says what is wrong; False or an exception also fails

Port("y", int, check=positive)                 # one function or several
```

The port decides what happens when a check fails:

| Setting | Effect |
|---|---|
| `invalid="fail"` (default) | The node fails and the run stops. The error names the node, round, port and reason. Once the problem is dealt with, `resume()` continues |
| `invalid="warn"` | The payload goes out as it is, the problem is recorded, and the run goes on |
| `repairs=N` | Before either of the above, the node is told about the problem so it can fix it and send again, at most N times per activation |

A repair goes one of two ways. A node that can fix things itself (an agent can) sees `ctx.emit` raise `Invalid`; it reads `problem` and, while `again` is true, fixes and emits again. If the node does not handle the exception, the engine runs it once more with the same inputs, with last time's problems in `ctx.problems`; but if it already emitted something else in this activation, it fails instead of rerunning, so that the same payload is not sent twice.

When `repairs` is not given, the node decides: 0 for plain nodes, 2 for `AgentNode`. When an agent is corrected, the problem's exact wording is given to the model as a message, and it still has all its tools, so it can go and write the missing file. A model that was asked for JSON and answered with something else goes the same way.

Every problem leaves a record, whatever its outcome, in four places: `Result.problems`, the log, events on the watch port (`rejected` means sent back to the node, `warned` means let through), and `problems` on the exception when a run fails. Each record is `{"node", "round", "port", "problem", "handled"}`.

On an input port only `invalid` applies: if what arrives differs from what was sent (for example, the file was changed on the way), a `fail` port makes the receiving node fail, and a `warn` port hands it over anyway and records the problem.

### Passing files by path

The `file` protocol lets nodes pass each other a file's relative path instead of its content. All nodes work in the same working directory; the sender writes the file under it and emits the path (a string, or a mapping with `path`, whose other keys travel along). The envelope holds the path plus the size, hash and type at the moment of sending: small plain data, journaled as usual.

```python
from interlacing.protocols import DEFAULT, FileOf, with_files

protocols = with_files(DEFAULT, "work")                       # working directory
Port("chart", FileOf(".png", ".svg", max_size=5_000_000))     # this port sends images, at most 5 MB
builder.connect("draw.chart", "report.chart", "file")
```

The receiver gets `{"path": "plots/a.png", "size": 1234, "sha256": "...", "media": "image/png"}`, and `protocols.get("file").locate(received)` is the file's full path.

On sending, these are checked in order: the payload is a path; the path is relative and, after following links, still inside the working directory; it is a file, not a directory and not missing; the file is not empty (`File(root, empty=True)` allows empty files); its suffix, type and size match the port's `FileOf`. On arrival it is checked again: the file is still there and its size and hash have not changed.

Error messages are written for the party that can fix them, for example `there is no file chart.svg in the working directory (the working directory holds: data)`.

What happens on a problem follows the port rules above exactly: by default the node fails and the run stops, resumable; with `invalid="warn"` the path is delivered anyway but carries `"unchecked": true`, so the receiver knows it was not verified; `repairs` gives the node a chance to supply the file. When the payload is not even a path, a warning cannot save it and the node fails.

`AgentNode(files=working_dir)` reads images that arrive as files and shows them to the model.

### Workspaces, workers and tool pools

`Workspace(directory)` holds all the files of one run; paths are written relative to its top, as with the `file` protocol. The layout is per node:

```
solver/            directory of node solver
shared/            writable by every node
review/            block review: one more level
    writer/        writer inside the block
    shared/        shared by the block's members
```

`workspace.area(node, layer=[blocks...], reads=None, writes=None)` is one node's share (an `Area`). By default it can read everything and write its own directory plus the `shared/` of each level it is in. Given `reads` and `writes` (lists of paths relative to the top), it follows those; anywhere it can write, it can also read. When loading a graph file, nodes in blocks get the extra level automatically, and `"reads"` and `"writes"` go on the node. Every file written through an area is recorded in `.interlacing/written.jsonl` (which node, which activation); `workspace.files()`, `Run.files()` and `interlacing files --dir RUN` list each file and where it came from.

Once a plain agent has an area (graph files give it one automatically), every path it emits from a file port must be one it can read; otherwise the path goes back to the model with the reason, to be fixed.

`WorkerNode(id, model, area_or_dir, commands=True, command_timeout=120, ...)` is an agent with its own `list_files`, `read_file`, `write_file`, `edit_file`, `delete_file` and `move_file`, plus `run_command` when `commands=True` (one shell command, run in its own directory; on timeout it is killed together with the processes it started). It is told where it is and where it may read and write. Everything else is as for an agent: ports, checks, repairs, steps, budget, time limit, and it can also take tools from the graph and from tool pools. Files changed by its commands are recorded under its name too. In graph files it is `"kind": "worker"`; with commands, the catalog must allow starting processes; `"commands": false` gives file tools only. File tools cannot leave the area, commands can: this is not a sandbox.

`ToolPool(directory, timeout=120)` is a tool pool. A tool is one file:

- `plot.py`: the function with the file's name (or a function called `run`, or the only function not starting with `_`). Its docstring is the description the model sees; its parameters, annotations and defaults become the tool's arguments. Getting the tool only reads the file and does not run it.
- `fetch.json`: `{"description": ..., "parameters": {JSON schema}, "command": ["./fetch.sh"]}`, any program; the arguments reach it as JSON on standard input, and what it writes out is the answer (read as JSON when it parses). Programs with relative paths are looked up in the pool.

`pool.tool("plot.py")` is one tool, `pool.tools("charts")` is every tool in a directory (`.py` and `.json` files not starting with `_` or `.`). Such tools belong to the node itself; they take no ports and need no edges. Each use runs in its own process, in the node's own directory, with the environment variable `INTERLACING_WORKSPACE` set to the workspace top; it is killed on timeout, and on failure the model is told why. In graph files, `"tools"` on agents and workers may hold paths (`["plot.py", "charts"]`); the tool pool itself comes from `Catalog(tools=directory)` or from `--tools directory` on the command line and the MCP server; a graph file cannot choose it.

## Graphs

`GraphBuilder` collects nodes and edges; `compile(protocols)` checks the whole and returns a read-only `Graph`. A graph's `digest` is the SHA-256 of its definition, so equal definitions have equal digests. Changing a graph means deriving a new graph from the old one:

```python
new = GraphBuilder.of(graph).remove("review").add(check).connect("draft.y", "check.x", "value").compile(DEFAULT)
```

### A switch on every node, and who manages whom

Every node in a run has a switch: on, paused or stopped. Nodes can be switched one at a time or in batches. There are four actions:

| Action | A node that is not running | A node that is running |
|---|---|---|
| `pause` | Does not start again | Finishes the current step and waits there, keeping its place and conversation |
| `resume` | Starts as usual and handles the messages waiting for it | Continues from where it stopped |
| `stop` | Does not start again; messages waiting for it are kept | Finishes the current step and ends this piece of work; the inputs it took are put back for later |
| `restart` | Forgets what it remembers (`Node.reset`; for an agent, the conversation is cleared) and starts afresh | As before, and redoes the work with its original inputs |

A "step" is a place where the node calls `ctx.checkpoint()`. `AgentNode` calls it before every model call and every tool use; a long-running node you write yourself should call it after each item and end when it returns `False` (coroutine nodes use `await ctx.acheckpoint()`). For a node that never calls it, the switch takes effect only when the activation is done. In that case its result counts as usual; if the action was `restart`, what it remembers is still cleared after the result counts. A node answering someone else's `call` waits at its next step when paused; when stopped, it finishes the answer, because the caller is waiting, and only then stops starting.

Nodes that hand their work to something outside follow the switch too. When a `RemoteNode` is paused, the remote node waits at its own next `checkpoint`, and when it is stopped the remote side is told. When an external program (`ProcessNode`, `FolderNode`, `IsolatedNode`, and the commands and tool programs an agent runs) is paused, on Linux and macOS the program itself is suspended (on Windows the program keeps running and only the node waits), and the program's own `timeout` does not count while it is paused. Nodes of this kind that you write can use `ctx.paused()` to know whether they are paused right now.

Who can flip a switch:

- The operator: `engine.control(node, action, team=False)`; for a run directory, `Run.control(...)` or `interlacing control ACTION NODE... --dir RUN` on the command line; on the MCP server, the `control` tool. While the run is going on it takes effect at once. When the run is going on in another process (for example an `interlacing run` in another terminal), the change is first put in the run directory's `controls/`, and that process picks it up within moments. When nothing is running it goes into the journal and takes effect at the next `resume`. The returned `changed` lists only nodes that really changed. With a bare `Engine` and no journal, a run that ended early because everything was paused cannot continue: to continue, use a run directory or pass `journal` to `Engine`.
- A node's own manager: in the graph, `GraphBuilder.manage("lead", "a", "b")` states who manages whom (in graph files, `"manages": {"lead": ["a", "b"]}`). This is a tree: a node has at most one manager, and no node can end up managing itself round a cycle. At run time a node may use `ctx.control(node, action, team=False)` only on nodes below it (any number of levels); anything else is refused. `team=True` also switches the nodes the target manages. `ctx.team()` lists the nodes below it and how each stands (switch, working or waiting, manager, rounds run). A node added during the run can be given a manager too: `Change().manage("lead", "new_node")`.
- Nodes in a graph that runs inside a node (`GraphNode`, blocks) are switched by path: `engine.control("box/slow", "stop")`, and further in, `box/inner/x`. Whoever can switch `box` (the operator, or the node managing `box`) can switch the nodes inside it. They can be reached only while `box` is running its graph.

- **Member failures go to the manager:** when a node's `oversees` is true (`"oversees": true` in graph files; agents that may hire have it by default), a failure of a node it manages does not fail the run. The failed node is stopped with its unused inputs kept; the failure is recorded in the problem list (`handled` is `reported`) and handed to the manager. The manager takes it with `ctx.failures()` (each with `node`, `round`, `error`); if it is waiting in `ctx.receive`, it is woken and gets an empty mapping. Then the manager decides: `restart` it, take it out of the graph, or fail itself so the whole run fails. The failure is handed over only while the manager is running; if the manager is not running, does not `oversee`, or ends without ever taking it, the run fails as before.

Every switch change goes into the journal, stays as it was after resuming, and watchers receive a `control` event. Time spent paused does not count against an activation's time limit. A paused node cannot be called (`call`).

When all the remaining work is paused, the run does not hang: nodes waiting at a pause put their inputs back, the run ends as "not finished", `Result.held` lists the nodes whose switches are not on, and the run directory's status shows `held` and `switches`. Nodes waiting on them (a lead waiting for a member's reply, say) end with the run too and are not started again within it, so the model is not asked again and again, and no one wraps up early for want of an answer. Switch them back on and `resume` to continue; it is the same idea as a run stopping first while a human in the graph has not answered yet.

### Receiving messages in the middle of an activation

A node does not have to get all its inputs when an activation starts. A running node can use `ctx.receive(ports=None, timeout=None)` to wait for its next message (coroutine nodes use `await ctx.areceive(...)`), and gets `{port: value}`. Messages come in the order they arrived; with `ports`, only those input ports are considered.

```python
def asker(ctx, x):
    ctx.emit("ask", x)                       # send the question first
    answer = ctx.receive("answer")           # wait for the answer without holding a worker
    return {"out": answer["answer"] if answer else None}
```

- **Waiting holds no worker.** `workers=N` limits activations that are working; waiting ones do not count. On the thread driver a worker thread is lent out, and the extra thread exits by itself once the wait is over. So even with `workers=0` or `1`, a node waiting for another node's answer does not get stuck.
- **When it returns `None`:** `timeout` seconds have passed; the node is stopped or restarted, or the run is ending (then `ctx.stopping()` is true); or no message can ever come, because every other activation has ended or is also waiting. When two nodes wait on each other, both get `None` and can each wrap up, so the run does not hang. If at that moment a stopped node still has work in hand, the run ends early as when everything is paused, and can be continued later.
- **Switches while waiting:** a paused node takes no new messages until it is resumed.
- **Time limit:** time spent waiting does not count against the activation's time limit.
- **Bookkeeping:** messages taken midway belong to this activation and are journaled together with the inputs it started with. When the activation is interrupted, they go back to the mailbox. Watchers receive a `received` event.

Messages that arrive while a node is running still trigger its next activation after it ends, as usual; nothing is lost. So a node can choose to keep receiving within one activation, or still be activated once per message.

### Saving the progress of a long activation

An activation can be long; a resident agent, for example, has only one activation from start to end. `ctx.commit()` writes what it has done so far to the journal, including the messages it took, the messages it sent and the node's own state (`snapshot`), and the activation then carries on.

- **After the process dies:** `resume` does not rerun the activation from the start. The node is first restored to its saved state, then run once with `ctx.resumed` true and no inputs, so it continues from there; messages that arrived in the meantime are collected with `receive`. Messages sent before the save are not sent again.
- **Resumed after a stop:** as after the process dies, it continues.
- **Restarted:** it does not continue but starts over with the inputs it got at the beginning.
- Messages taken before a save are used up and are not put back on a stop.
- Without a journal, `commit` returns `False` and nothing else changes.
- A node that uses `commit` needs a `run` that handles `ctx.resumed`.

```python
def run(self, ctx, inputs):
    if not ctx.resumed:
        self.todo = make_list(inputs)
    while self.todo and ctx.checkpoint():
        ctx.emit("out", work_on(self.todo.pop(0)))
        ctx.commit()                         # self.todo is in the snapshot
```

### Resident agents

An agent with `AgentNode(..., resident=True)` (`"resident": true` in graph files) does not "get a message, answer once, and end". It stays until it says itself that it is done. The whole stay is one activation. It has these tools on top of a plain agent's:

| Tool | Purpose |
|---|---|
| `send_<output>` | Present even with a single output. Send each part as soon as it is ready, without waiting for everything |
| `wait(seconds?)` | Wait for messages from others (`ctx.receive`) without holding a worker. Returns what arrived and the task board; if no more messages can come (everyone else is done or also waiting), it says so plainly |
| `plan(items)` / `done(item)` | Add items to the task board and tick them off (by number or text) |
| `note(text)` | Write down how far it has got, and save progress at the same time (`ctx.commit`) |
| `correct(id, content)` | Correct a message it sent. `id` is the id that `send_<output>` gave back (such as `say#2`); see "Corrections" below |
| `explain(id, text)` | What it sent was rejected but is right: give the reason. See "Rejections" below |
| `hire` / `dismiss` / `control` / `team` / `connect` / `fail` | Only when hiring is on (`hire=N`); see "Hiring" below |
| `finish(summary?)` | Declare it is done. If some output has never been sent, the first call only reminds it; a second call ends it |

- **Task board:** at the start each output has a line ("not sent yet" / "sent N times", plus how many times it was corrected), then the items the agent added with `plan`, and last its latest note. It comes with every `wait` result, is part of the node's state, and is saved.
- **Saving:** progress is saved before every `wait` and on every `note`. After the process dies, `resume` lets the agent continue with its old conversation and board; the model first sees a line saying the run was interrupted and picked up again, plus its board.
- **When it calls no tool:** a model that only replies with text is reminded to wait, send or finish, and does not stop there. `max_steps` limits the number of model calls between two received messages.
- **Switches:** like any other node it can be paused, stopped and restarted; its manager can switch it with `ctx.control`.

[examples/graphs/resident_team.py](../examples/graphs/resident_team.py) has a lead with two resident members: the lead hands out work, waits for both reports, checks them with a plain function tool, and delivers the result; each member sends its report as soon as it is done, waits, and ends once it learns that no more messages will come.

### Corrections

A node that sent a wrong message can correct it. `ctx.emit` returns the message's id (`"output_port#n"`, such as `"out#3"`); later, `ctx.correct(id, new_payload)` puts the new payload in its place:

```python
def survey(ctx, site):
    quick = ctx.emit("area", rough(site))     # send an estimate first so others can start
    ctx.receive()                             # wait until those who read it are done with it
    ctx.correct(quick, careful(site))         # then replace it with the careful measurement
```

The correction follows the message wherever it went:

- **Not yet read:** it is swapped in the mailbox, and the receiver only ever sees the corrected payload.
- **Already read:** the receiving node's `on_correction` decides:
  - `rerun` (the default, suited to program nodes): the activation that read it runs again with the corrected value and its other inputs unchanged. The k-th message sent by the rerun automatically counts as a correction of the k-th message from the first run, so the correction flows on downstream; extra messages are new messages as usual, and messages it no longer sends stay as they were.
  - `tell` (the default for `AgentNode`): the correction arrives as a new message, and `ctx.replaces(port)` says which message it corrects; the agent sees it marked as a correction of a message it received before, and decides for itself what to do. A message read midway with `ctx.receive` is always corrected as `tell`.
  - `ignore`: nothing happens.
- **Graph outputs:** the entry in the result is replaced in place; no extra entry appears.

Rules:

- A node can correct only its own messages; the same message can be corrected several times, and the last one counts. Each node remembers its latest 1024 correctable messages; correcting an older one is an error.
- Corrections are checked like ordinary messages and count against the edge's `max_rounds` the same way: once an edge is full, corrections are no longer sent.
- Corrections are journaled. After a run is resumed, unread corrections are still in the mailbox; a message that was already read and is corrected afterwards is delivered as `tell` (the inputs needed to rerun an activation are not journaled).
- In graph files write `"on_correction": "tell"`; it appears in the description only when it differs from the default for that kind of node.
- Resident agents correct with the `correct(id, content)` tool; `send_<output>` gives back ids like `sent, as say#2`.
- Messages on stream and call edges cannot be corrected. Remote nodes (`RemoteNode`) can correct their own messages too: the remote `ctx.emit` returns the remote side's own id, `ctx.correct` uses it, and the local side maps it to the id in the run.

[examples/graphs/corrected_estimate.py](../examples/graphs/corrected_estimate.py): a rough estimate is sent first and corrected once its readers are done with it; the pricing program recomputes automatically and the quote is fixed in place; the agent writing the note is told and writes a new one.

### Rejections: downstream pushes back, upstream answers

Downstream does not have to accept everything. Once an edge allows rejections, the receiving side can say "this is not what I expected" and reject it, and upstream has to explain or correct:

```python
builder.connect("planner.plan", "reviewer.plan", challenges=2)    # this edge accepts at most 2 rejections
```

- **The switch is on the edge:** without `challenges` it is off, as before. With it, it is on, and the number is how many rejections the edge accepts in total. Stream, call and non-triggering edges cannot have it.
- **Downstream only says yes or no:** it does not need to give a number. If what came is what it expected, it uses it as usual; if not, it calls `ctx.reject(port, reason)`, which rejects the last message this activation took on that port, and then waits (holding no worker). The result is one of three:
  - `{"answer": "explained", "text": ...}`: upstream explained; the value stands.
  - `{"answer": "corrected", "value": ...}`: upstream corrected it; this is the new value. This correction is the answer to the rejection and is not handled again through `on_correction`; other nodes that read the message follow their own `on_correction` as usual.
  - `{"answer": "refused", "why": ...}`: the edge has no rejections left (this is also recorded in the problem list), upstream is stopped or gone (if upstream is stopped or removed after the rejection was sent, even while it is answering, this result comes at once), the run is ending, or nobody will ever answer. Downstream then has to go on with the value it has. An activation rerun because of a correction can also reject the new value it read.
  - `ctx.challengeable(port)` says whether it can still reject now.
- **Rejecting a specific message:** when an input port is fed by several edges (`many=True`), `ctx.reject(port, reason, which)` names the one being rejected with `which`: the sending node's id, the edge id or the envelope id all work; without it, the last one taken is meant. `ctx.taken(port)` lists each message this activation took on the port (`id`, `from`, `edge`). Each edge's count is kept separately.
- **How upstream answers:**
  - Running and waiting in `ctx.receive`: it is woken, `receive` returns an empty mapping, and `ctx.rejections()` gives the rejections (`id`, `port`, `message`, `content`, `reason`, `by`, `sent`). It answers with `ctx.explain(rejection_id, text)` or `ctx.correct(message, new_value)`.
  - Already ended, or this activation did not answer: the engine starts a separate activation for it and calls `Node.challenged(ctx, rejection)`. It can correct, or return an explanation. A program node answers by default that the value was computed from its inputs and stands; write your own `challenged` for anything else.
- **Other messages sent in the same activation:** `sent` in a rejection maps output ports to lists of ids, listing what else the activation that sent the rejected message sent (at most 32). For example, `a` has two outputs, 1 and 2, and 2 is rejected: `a` can see that what it sent on 1 was `one#1`, judge whether it is wrong for the same reason, and if so `ctx.correct` it as well. Downstream of 1, nodes rerun or are told according to their own `on_correction`, and the correction flows further down from there.
- **Agents:**
  - As downstream: a message that can be rejected comes with a default prompt asking it to judge first whether it is what it expected and to reject it if not, plus a `reject(port, reason, which)` tool; when the port is fed by several edges, the prompt says who sent each one, and `which` takes the sender. The prompt can be replaced with your own words: `AgentNode(..., doubt="...")`, with `{ports}` standing for the port names.
  - As upstream: when rejected, the model sees a `[rejected]` message with who rejected it, the reason and what the other side received. It answers with `explain` or `correct`. A resident agent gets it in `wait` and has the tools `explain(id, text)` and `correct(id, content)`; a plain agent is called back and answers with its old conversation. The `[rejected]` message also lists the ids of the other messages sent at the same time, prompting it to check whether they need fixing too; `correct` with an `id` can correct any of them.
- **Journal:** each rejection is recorded under the downstream activation, so after a run is resumed the edge's used count is not reset.

[examples/graphs/rejected_answer.py](../examples/graphs/rejected_answer.py): a reviewing agent rejects a plan that misses a step; the planning agent has already ended, is called back, and corrects it; the reviewer gets the corrected plan and approves it.

[examples/graphs/directed_rejection.py](../examples/graphs/directed_rejection.py): two estimates meet at one port; the merging program rejects the quick one by name (`which="quick"`); the quick one sees in the rejection that it also sent a note at the same time, and corrects both the estimate and the note; the log node that already read the note reruns with the new note.

### Hiring: an agent builds its own team

A resident agent can hire members during the run, each level managing the next. It is off by default; to turn it on, give a limit:

```python
lead = AgentNode("lead", model, resident=True, hire=3, templates={"researcher": make_researcher})
```

- **Switch and limit:** `hire=3` means at most 3 hires in the whole run; without it, or with 0, it is off and the tools are not even there. Dismissing does not free a place. Only resident agents can hire, because they must stay to hear their members' replies.
- **Hiring only from templates:** `templates` maps names to templates. A template is either a function that builds a node for a given id, or a node description. In graph files write `"hire": 3, "templates": ["researcher"]`, with names pointing to descriptions registered in `Catalog(templates={...})`; descriptions can also be written directly, `{"researcher": {...}}`. A member that can hire too never gets a `hire` larger than its hirer's.
- **Tools:**

| Tool | Purpose |
|---|---|
| `hire(template, name, task?)` | Hire a member from a template; its id is `<self>-<name>`. It is managed by the hirer, which gains an output `to_<name>` (send it work with `send_to_<name>`) and an input `from_<name>` (the member's first output comes back on a back edge, at most `hire_rounds` rounds, 10 by default). With `task`, the work is sent at once |
| `dismiss(name)` | Stop this member and everyone below it, then take them out of the graph; one that is still working is removed once it has stopped (before the next model call), and until then the name cannot be reused |
| `control(name, action)` | Pause, resume, stop or restart a member (together with those below it); `name` may be a path that reaches into a member's inner graph |
| `team()` | List the team: each member's switch, whether it is working or waiting, rounds run, who manages it, and how many hires are left |
| `connect(source, target, back?)` | Wire between itself (`me`) and its members, written as `name.port`; a wire that closes a cycle needs `back=true`. Nodes outside the team cannot be reached |
| `fail(reason)` | Decide the work cannot be done and make the whole run fail with this reason |

- **Separate directories:** with a workspace, a member hired from a description gets a directory under its hirer's (`lead/lead-r1/`); it can write only its own directory and the shared ones, and its hirer can read it.
- **What is underneath:** all of this is `ctx.change`: checked first, then applied, journaled, and redone when a run is resumed. A member remembers who hired it from which template (`made_by` in its description), and on resume the agent that hired it makes it again from the same template, so `Engine(..., journal=...)` in Python needs no `rebuild` either. There are two new `Change` operations: `ports(node, inputs=, outputs=)`, which gives an existing node more ports during the run (added ports are not type-checked; the node learns of them through `Node.add_ports`), and `manage(manager, members...)`. When a whole change is refused, the added ports are taken back too.
- **Member failures:** an agent that may hire takes over its members' failures (see "Member failures go to the manager" under the node switches). In `wait` it sees a `[failed]` line with who failed, in which round and with what error, and it can restart the member with `control`, dismiss it with `dismiss`, or give up with `fail`.
- **Still only along wired edges:** hiring only adds nodes and edges; there is no "send a message to someone by name".

[examples/graphs/hired_team.py](../examples/graphs/hired_team.py): the graph holds only a lead. It hires two word-counting members, gives each half, waits for both results, looks at its team, dismisses both and delivers the total; a third hire is refused.

## Methods

A method takes nodes and returns a graph. The rules of the run are already in the graph: when each node fires, and how many rounds each back edge allows at most.

| Method | Structure |
|---|---|
| `Chain(a, b, c)` | a → b → c |
| `Loop(a, b, until=f, max_rounds=N)` | The end of the chain goes back to the start until `f` holds or the rounds run out |
| `Fan(a, b, c)` | The same input goes to every branch and the results are joined. When ports of `split` or `join` are named after branch ids (with `-` in an id written as `_`), each branch gets and returns its own share; when `split` has several inputs or `join` has several outputs, they become the graph's inputs and outputs |
| `Tree(lead, a, Tree(b, b1, b2), max_rounds=N)` | A coordinator hands tasks to those below it, and their replies come back to the coordinator; any number of levels |
| `Star(hub, a, b, c, max_rounds=N)` | A tree of one level |
| `Governed(lead, a, b, operator=person, max_rounds=N)` | A tree in which the coordinator asks an operator node for approval before releasing |
| `Pool(lead, member, size=10, max_rounds=N)` | The coordinator calls up members as needed, at most `size` of them |

`Pool` is for "up to ten, usually only three or four". `member(id)` is a function that builds a member. The graph has `size` places, but a place does nothing until the coordinator hands it work: no activation, no model request, no conversation, and nothing in the journal. The coordinator sends `{"to": "geometry", "task": ...}` from `send`: an unused name calls up a new member, a used name finds the same member as before, and the rest of the mapping (a role, for example) goes to the member with the task, so one template can make members that do different things. Anything without `to` calls up a new member each time. Replies come back to the coordinator as `{"from": name, "reply": ...}`; once the places are used up, a new name gets `{"from": name, "error": ...}`. A `Pool` can sit in a lower position of a `Tree`, or inside `Governed`.

Your own methods subclass `Method` and implement `build(protocols)`.

## Blocks

A node is a thing in a particular graph, with its own name and state, so the same node cannot appear in two places. A block is the design itself: a recipe that builds the graph afresh each time, so each use gives an independent node.

```python
from interlacing import Block, seq, par

review = Block(lambda: Loop(writer(), critic(), max_rounds=3), name="review")

Chain(outline, review("first-pass"), review("second-pass"), publish)     # used twice, each with its own state
Fan(*review.times(4))                                                    # four at once: review-1 to review-4
```

One use of a block shows to the outside only the input and output ports of the graph inside.

Blocks combine into larger blocks without naming every node:

```python
pipeline = seq(clean, review, publish)             # one after another: a chain
panel = par(review, review, review)                # the same input to each, outputs joined into a list
study = seq(clean, par(review, review), publish)   # they nest
```

Each part of `seq` and `par` can be a block or a function that builds a node from a name. A node that is already built also works, but then the combined block can be used only once, because that node cannot be built again.

The run inside a block belongs to the run outside:

- When the outside has a journal, the inside run keeps its own journal next to it (`<journal>.d/<node>-<round>.jsonl`). After an interruption the block continues from where the inside stopped, and nodes that finished are not redone. Once the block's activation is in the outside journal, the inside journal is deleted.
- The watch port receives the inside events; `within` on an event says which block it is in, with one name per level of nesting.
- Time limits apply to each node inside, not to the block as a whole.
- Output problems found inside are recorded outside too, with the node named `block/node`.
- A block can contain a human: the run stops waiting, and after the answer it continues inside the block.

A block can also be saved as a graph file and referred to from other graphs, so one design can serve many graphs:

```json
{"method": "fan", "nodes": [
  {"id": "review", "kind": "block", "file": "blocks/review.json", "times": 4,
   "defaults": {"agent": {"model": "anthropic:some-model"}}}
]}
```

`file` is the block's graph file, read from a directory that `Catalog(files=...)` allows. `defaults` written where the block is used override the block file's own `defaults`, so the same design can be used with other settings; settings written on the block's nodes themselves are not affected. `times` works on any node and builds `name-1` to `name-n` at once. In Python, `interlacing.spec.block("blocks/review.json", catalog)` reads the file into a `Block`.

A node can also run a graph inside one activation: `ctx.engine(graph, protocols).go(inputs)`. All the properties above come from the engine, and that is how `GraphNode` is built.

## Growing during a run

When what needs doing is only known partway through a run, there are two ways: change this graph itself (`ctx.change`), or have a node write the graph it needs as data and hand it to a `SpawnNode`, which runs it within one activation.

### Changing this graph: `ctx.change`

During a run, a graph can be changed only by an explicit operation. A node hands over a `Change` during an activation:

```python
from interlacing import Change

def plan(ctx, task):
    change = Change()
    for i, part in enumerate(split(task)):
        change.add(make_solver(f"s{i}")).connect("plan.y", f"s{i}.x").output(f"s{i}", f"s{i}.y")
    ctx.change(change)
    ctx.emit("y", task)
```

- The operations are `add(nodes...)`, `remove(node_ids...)`, `connect(source, target, protocol="value", back=, max_rounds=, challenges=)`, `disconnect(edge_ids...)`, `output(name, source)`, `ports(node, inputs=, outputs=)` and `manage(manager, members...)`, applied in order to the current graph. Removing a node removes its edges and graph outputs along with it.
- Once applied, the result is checked with all the rules for building a graph. If it fails, `ctx.change` raises `GraphError` and the graph does not change at all, so the node can catch it and try another way; if it passes it takes effect at once: new nodes start running by their own firing rules, new edges start delivering, and envelopes still waiting on removed edges are dropped, with the count reported to watchers.
- A node cannot remove itself or a node that is running. `Engine(max_nodes=N)` caps how large a graph can grow.
- Watchers receive `changed` (`node`, `round`, `added`, `removed`, `dropped`, `graph` with the new digest); `Engine.graph` is the current graph, and `Result` holds the new nodes' rounds and the new graph outputs.
- With a journal, the change is written as plain data together with the activation that proposed it, and on resume it is redone in the original order, so a grown graph is still there after resuming. The journal stores a new node's description, which has to be turned back into a node on resume: `Engine(..., rebuild=function)`, for example `lambda d: load_node(d, Catalog(allow=[...]))`; run directories (`Run`, the command line, the MCP server) do this automatically using the graph file's directory. Without `rebuild`, a run with a journal can add only members an agent hires (the agent makes them again from its templates), and no other nodes. Blocks and `GraphNode` cannot be added this way, because their descriptions do not contain the graph inside.

The difference: `ctx.change` changes this graph, and new nodes stay like any other node, can be found again and again, and can be wired to existing nodes; a graph run by `SpawnNode` lives for one activation only.

### Running a new graph: `SpawnNode`

Have a node write the graph it needs as data and hand it to `SpawnNode`: it builds the graph from the description, runs it and sends the result back. The new graph exists only within one activation of this node; as with the inside of a block, the outer graph does not gain a single node.

```python
from interlacing.spec import Catalog, SpawnNode

grow = SpawnNode("grow", Catalog(allow=["my_steps"], models=models), max_nodes=20)
graph = Chain(planner, grow).build(DEFAULT)
```

The `request` port takes `{"graph": description, "inputs": {...}}`; the description is written exactly like a graph file, with methods, `defaults`, `times` and blocks all available. The `result` port emits `{"outputs": {...}, "rounds": {...}, "problems": [...]}`. Wired with a call edge it is a tool: an agent calls it with a graph and gets the result as the answer, so a coordinator can decide for itself how many members to send out this time and how to wire them.

What the new graph can reach is decided by the `Catalog` given when this node was built, not by whoever writes the graph: only functions, models and programs the catalog allows, and files only from places it allows. Nodes in the new graph cannot give themselves longer time limits than outside, and `SpawnNode`s and blocks nested in the new graph cannot ask for more nodes or threads than this node has. There are three more limits:

| Setting | Meaning | Default |
|---|---|---|
| `max_nodes` | The most nodes one graph may have, counting those in blocks | 50 |
| `depth` | How many more levels of `SpawnNode` the new graph may nest | 0, not allowed |
| `max_runs` | The most graphs run in one outer run | Unlimited |

To give it to an agent as a tool, use `grow.tool()`: the description of the `Tool` it returns already holds the graph format (`GUIDE`) and the functions this node's catalog allows, with their signatures, so the model needs no other prompt to write a graph:

```python
agent = AgentNode("designer", model, tools=[grow.tool()])
graph = GraphBuilder().add(agent, grow).connect("designer.grow", "grow.request", "call")...
```

When the description cannot be built, a limit is exceeded or the new graph fails, by default `{"error": ...}` is sent back as the result, so the sender can try writing it another way; an agent sees the reason in the tool's answer. With `strict=True` the node fails instead, and the whole run with it.

The new graph's run is journaled, resumable and time-limited like a block's, and the events watchers receive carry `within`. When a human in the new graph needs to answer, the question id includes where it is, for example `grow.1__first-1` is the first question of `first` in the first activation of `grow`, so humans with the same name in different graphs are not mixed up.

In a graph file it is `{"id": "grow", "kind": "spawn", "max_nodes": 20}`, using the catalog that reads that file.

### Choosing a way to put a graph inside a graph

These ways differ in only two things: when the structure inside is fixed, and how long what is inside lives.

| | Structure fixed | Lives | Use for |
|---|---|---|---|
| `GraphNode` | When building | Runs the inner graph once per activation | An existing graph as one node; the other ways are built on it or on the same mechanism |
| `Block` | When building | As above, each use with its own state | One design used many times, or combined |
| `Pool` | Places reserved when building | The whole run; members remember their conversations | An unknown number of members, each of which must be found again |
| `SpawnNode` | At run time | One activation, gone afterwards | Even the structure is only known during the run |
| `ctx.change` | At run time | The whole run, like nodes present from the start | The structure is only known during the run, and new nodes must be found later and wired to others |

`Pool` places are not reclaimed on purpose: the member in a place carries its own memory, and giving the place to a new name would mean a new member carrying on an old conversation. For members that "go when done and are brand new next time", use `SpawnNode`; for members that "can be found again later", use `Pool`.

## The engine

`Engine(graph, protocols, workers=0, journal=None, transport=None, time_limit=None, observe=None).run(inputs)` returns a `Result`: everything each graph output received, how many times each node ran, the back edges that hit their limits, and whether the run finished.

Scheduling is event-driven. Each edge has its own mailbox and each node tracks how many required inputs it still lacks; an arriving envelope changes only one node's count, so the scheduling cost per message does not depend on the size of the graph (measured at about 9 microseconds per envelope from 10 to 10,000 nodes). `workers=0` runs in the calling thread in a fixed order; with more worker threads nodes run in parallel, and the same node never runs twice at once. When several threads hand off between tiny nodes that take a few microseconds each, an envelope costs 60 to 100 microseconds; that is the price of Python thread handoffs, negligible next to a model call that takes seconds. Coroutine nodes take about 20 to 30 microseconds per activation on the event loop.

`journal=Journal(path)` makes a run resumable: every completed activation is written whole to the journal, and a new engine with the same journal continues with `resume()`. What finished is not redone; what was interrupted is rerun. `resume(change=True)` lets the run continue under a different graph.

`time_limit` is the most seconds one activation may take; a node's own `time_limit` attribute takes precedence (`HumanNode` has no limit by default). Threads cannot be killed, so a timeout is handled like this: the run ends in failure naming the node, the other nodes see `ctx.stopping()`, the engine stops waiting for the stuck node, and anything it does afterwards is discarded. The timed-out activation is not journaled, and `resume()` runs it again.

`observe` is a function the engine tells about each thing as it happens (for a failure handed to a manager, the `failed` event has an extra `to`): `started`, `emitted`, `corrected`, `answered` (a rejection got its answer), `received`, `saved`, `finished`, `failed`, `stopped`, `used`, `changed` (the graph changed during the run) (all with `node` and `round`), `control` (a switch was flipped, with `nodes` and `action`, plus `by` when a node flipped it), and finally `ended`. A UI or log that follows a run live hooks in here instead of reading the journal file. It is called in the thread running the node and should not raise; if it does, the run fails and says why.

When a run is stopped (a node failed, a timeout, the budget ran out, or a stop was requested from outside), running nodes learn of it from `ctx.stopping()`. An activation that asked and got "yes" may have done only half its work, so the engine does not record it as completed: its inputs stay where they are, it is rerun on `resume()`, and watchers receive `stopped` rather than `finished`. An activation that returns normally without asking `ctx.stopping()` is recorded as completed as usual. That is why graphs inside blocks and `SpawnNode` never leave behind a record that says "finished but no output" after an interruption.

`firm=True` is for graphs you did not write yourself: `time_limit` is then a hard limit, and a longer `time_limit` a node writes for itself does not count (except for nodes waiting on a human and nodes holding another graph, whose inner nodes are timed one by one). Graphs run by `SpawnNode` are always treated this way.

`budget` is the usage limit for the whole run. Nodes report what they used with `ctx.use(tokens=1200, calls=1)`; the names are the node's choice, and the engine just adds them up by name. `AgentNode` reports `calls`, `input_tokens`, `output_tokens` and `tokens` for every model call. With `Engine(..., budget={"tokens": 200000})`, as soon as any total exceeds its limit the run ends in failure saying how much was used, even if the node does not stop by itself. What is used inside blocks and `SpawnNode` counts toward the outer run. The totals are in `Result.used`, on failure in `RunError.used`, and watchers receive `used` events. Usage is journaled entry by entry, so totals keep adding up after a resume; an activation rerun after an interruption cost money twice and is counted twice. A run that failed on its budget needs a larger budget to `resume()`.

`ctx.key` is the name of this activation, of the form `run_id/node#round`; inside a block it includes the outer position. The same activation keeps its name when rerun after an interruption, and different runs have different names. Resuming is "at least once", so when a node does something outside the graph that must not be repeated (placing an order, sending a message, writing to a database), it should pass this name to the other side as an idempotency key, or check first whether this name was already done. The kernel only provides the name; it does not deduplicate for the node.

`ctx.once(name, do)` does one thing outside the graph once: the return value of `do()` is journaled under `name`, and when this activation is rerun (continuing after an interrupted run, or redone after an emitted payload was sent back) the recorded value comes back without calling `do` again. The return value must be plain data. If the process dies exactly between "done" and "recorded", it is still redone, so the safest way is to use both: `ctx.once("charge", lambda: bank.charge(order, key=ctx.key))`.

Nodes can be coroutines: `run` (and `open`, `close`) written as `async def`, or an `async def` function given to `FunctionNode` or `@node`. When a graph has such nodes, the engine runs on an event loop: coroutine nodes are awaited on the loop and take no thread, and plain nodes go into threads as before. `workers` is still the limit on simultaneous activations, and `workers=0` means one at a time in a fixed order, so a hundred coroutine nodes all waiting for model answers can wait together with `workers=100` without a hundred extra threads. `ctx.emit`, `ctx.use` and `ctx.change` are called directly in coroutine nodes as usual; call edges need `await ctx.acall(port, payload)`, and using `ctx.call` from a coroutine node to call another coroutine node is refused. A coroutine node that times out is really cancelled (it receives `CancelledError`), unlike a node in a thread, which can only be abandoned.

A caller that is a coroutine uses `await engine.arun(inputs)`, `aresume()` and `ago()`. With coroutine nodes in the graph, the run happens on the caller's event loop; without them it runs in threads, and the event loop is not blocked either. Cancelling the await stops the run, and coroutine nodes being awaited are cancelled. A synchronous `run()` on a graph with coroutine nodes opens an event loop itself; if the calling thread already has one running, it opens it in another thread. A coroutine node that runs an inner graph uses `await ctx.engine(graph).arun(...)`, which runs on the same loop.

`transport` is the interface between sending and receiving; it is not needed within one process, and ways of delivering across processes or machines go here.

Inside, the engine is split into parts by responsibility, and `Engine` only strings them together. The parts are not public and their interfaces may change:

| Part | Handles |
|---|---|
| `wiring` | The graph as it is at run time: each edge's mailbox, each input port, each node's state; built once when the run starts |
| `scheduler` | Putting envelopes in and taking them out, how many inputs a node still lacks, the ready queue |
| `checks` | Checks on sending and on arrival, sending back for repair, letting warnings through, recording problems |
| `ledger` | Usage totals and budgets, results recorded by `ctx.once` |
| `recorder` | What is written to the journal, and resuming a run from it |
| `clock` | Each node's time limit, and which activation has timed out |
| `context` | The `ctx` a node gets |

## Graphs as data

`interlacing.spec` writes a graph as JSON and builds the same graph back from it:

```python
from interlacing.spec import Catalog, dump, load, read, write

write("graph.json", graph)                                   # the graph's definition plus those of nested graphs
again = read("graph.json", Catalog(allow=["mypkg"]))         # again.digest == graph.digest
```

What is written is the graph's own description (the digest is computed from it): each node's kind, ports and configuration, every edge, and the graph's inputs and outputs. Functions are named with their module (`mypkg.steps.clean`), so they must be top-level functions in a module; a `lambda` cannot be written out. Models are named.

A description holds only names, and building the graph turns names into real things, which amounts to letting the description decide what code runs. `Catalog` is where the host sets the bounds, and by default nothing can be reached:

| Parameter | Meaning |
|---|---|
| `allow` | Which modules functions may come from; `"mypkg"` includes every module under it |
| `processes` | Whether `process` nodes are allowed; they start programs on this machine |
| `models` | `models(name)` returns the model for an agent; without it agents cannot be built |
| `ask` | `ask(node_id, round, request)` answers for `human` nodes in the graph that have no function of their own |
| `protocols` | The protocol table used to compile the graph |
| `files` | Which directory a description may take text from; without it no file is read, and files outside the directory are always refused |
| `defaults` | The host's default settings for each kind of node |

`catalog.register(kind, loader)` adds your own kinds of node.

`allow` lets through only functions **defined** in an allowed module (a class, or an object with `__call__`, counts where its class was defined): `mypkg.os.system`, reaching through an allowed module into a module it imported, is refused, and so is a function imported from elsewhere (a function wrapped by `@node` counts as the function it wraps). Graphs written by a model and graphs sent by an MCP client are held to this too.

### The format, briefly: `GUIDE` and `functions`

`interlacing.spec.GUIDE` is the graph format on one page, written for a person or a model writing a graph for the first time; `functions(catalog)` lists the functions a catalog allows, each with its signature and the first line of its documentation; `guide(catalog)` is both together. The MCP server has a `guide` tool of the same content, the command line has `interlacing guide --allow mypkg`, and `SpawnNode.tool()` carries it in its description.

`GUIDE` also lists the ports of the common kinds of node (`human` is `request` to `answer`, `branch` is `x` to `yes`/`no`, and so on), and a test checks it against the real ports. What is most often written wrong is in `GUIDE`: an edge is `{"from": "a.out", "to": "b.text"}`; a method's graph input is `in` and its output `out`; a fan's output is the list of its branches' outputs; a node of a method may itself be a method. A part written in the wrong shape is answered with the right shape; a wrong port or graph input is answered with the ones there are.

### Writing only what differs

When building your own framework, a graph file needs only the structure and the few settings where you do not want the defaults.

`defaults` gives one kind of node shared settings, and each node writes only what differs. The order of precedence is: what the node writes itself, then the description's `defaults`, then `Catalog(defaults=...)`, and last the package defaults.

Any setting can, instead of being written in the description, point to a file next to it: `{"file": "prompts/lead.md"}`. To change a prompt, write a file and refer to it like this. The command line allows reading files in the graph file's directory by default.

Each node can have a `time_limit` (seconds), which can also go in `defaults`.

```json
{
  "defaults": {"agent": {"model": "anthropic:some-model", "max_steps": 20, "time_limit": 600}},
  "method": "pool", "size": 10, "max_rounds": 3, "places": "scout",
  "root": {"id": "lead", "kind": "agent", "system": {"file": "prompts/lead.md"}, "firing": "any",
           "inputs": ["task", {"name": "reply", "required": false}], "outputs": ["out", "send"]},
  "member": {"kind": "agent", "system": {"file": "prompts/scout.md"}}
}
```

Port checks are written like this in graph files; check functions are named with their module like any other function:

```json
{"name": "chart", "schema": {"file": {"suffixes": [".png"], "max_size": 5000000}}, "check": "mypkg.checks.has_axes",
 "invalid": "warn", "repairs": 2}
```

What `dump` writes no longer contains `defaults` or file references: the defaults and the text of the files have already landed on each node, so the graph saved in a run directory does not depend on the original files.

In Python none of this is needed: a prompt is just a string, `AgentNode("lead", model, system=Path("prompts/lead.md").read_text())`.

Written by hand it can be shorter: node settings go right next to `id` and `kind`, ports are just names, and edges are just `from` and `to`; or skip nodes and edges and name a method directly:

```json
{"method": "loop", "max_rounds": 4, "until": "mypkg.checks.good_enough", "nodes": [
  {"id": "draft", "kind": "agent", "model": "anthropic:some-model", "system": "..."},
  {"id": "tidy", "kind": "function", "function": "mypkg.steps.tidy"}
]}
```

A function node reads its ports from the function, as in Python: each parameter is an input, the output is `out`, and a first parameter called `ctx` is the context, not an input. Write `inputs`, `outputs` (or `"context": true/false`) only to rename them or add checks.

A node of a method may be written as another method; it becomes a node holding that graph, named by its `id`, or else by the method (`fan`, `fan_2`...). It is the same as `{"kind": "graph", "graph": {...}}`:

```json
{"method": "chain", "nodes": [
  {"id": "clean", "kind": "function", "function": "mypkg.steps.clean"},
  {"method": "fan", "nodes": [{"id": "count", "kind": "function", "function": "mypkg.steps.count"},
                              {"id": "top", "kind": "function", "function": "mypkg.steps.top"}]},
  {"id": "report", "kind": "function", "function": "mypkg.steps.report"}
]}
```

## Command line and run directories

Installing gives the command `interlacing` (or use `python -m interlacing.cli`). Every command prints JSON.

| Command | Purpose |
|---|---|
| `guide [--allow module]...` | Print the graph format and the functions these modules let a graph name (this one prints text, not JSON) |
| `check graph.json` | Build the graph without running it, and print the digest, nodes, inputs and outputs; needs no keys |
| `run graph.json --dir RUN` | Start a run in a new directory; `--input name=value` (the value is read as JSON, or as text if that fails) or `--inputs file`; `--time-limit seconds` is the time limit per activation; `--budget name=number` (repeatable) is the usage limit for the whole run, for example `--budget tokens=200000`; `--workspace dir` is the nodes' working directory, by default `files/` in the run directory |
| `resume --dir RUN` | Keeps the bounds the run was started with (`--allow` and the rest are not given again) and continues from the journal: after a human answered, after a failure was fixed, after an interruption; `--budget name=number` continues with a larger budget |
| `answer --dir RUN [question] --json value` or `--text text` | Answer a question put to a human in the graph; with only one open question, its name can be left out. The run still waits afterwards; `next` in the output says to `resume` it |
| `status --dir RUN` | Print the run's status and open questions |
| `files --dir RUN` | List the files in the workspace, and which node wrote each one in which activation |
| `control ACTION NODE... --dir RUN [--team]` | Pause, resume, stop or restart nodes, see "A switch on every node, and who manages whom"; when the run is not going on, it goes into the journal and takes effect at the next `resume` |
| `node-serve module:name` | Serve one node in this process for a `RemoteNode` elsewhere; `--listen host:port`, `--token-env VAR`, `--make`, see "Nodes in another process or on another machine" |

The directory the command is run in is put on the module search path (as `python -m` does), so a `steps.py` beside it can be named with `--allow steps`. Output closed early (`| head`) is not an error.

`check` and `run` use `--allow module` (repeatable), `--processes`, `--tools tool_pool_dir`, `--apps app_dir` and `--remote address` to set what the graph can reach, with the same meaning as in `Catalog`; without them nothing can be reached. The bounds are written into the run directory at `run`, and `resume` keeps them. `--workers N` is the number of threads running nodes in parallel.

The exit code of `run` and `resume` says where the run stopped: 0 finished, 1 failed or refused, 3 waiting for a human's answer, 4 stopped by rounds or a stop condition.

Everything about a run is in its directory, so one process can start it, another can answer and a third can continue:

```
graph.json      the graph's definition
options.json    what the graph can reach, threads, time limit and budget
inputs.json     the inputs given when the run started
journal.jsonl   what has happened; blocks and graphs grown during the run have their own journals under
                journal.jsonl.d/, deleted once their activation completes
questions/      questions put to humans in the graph, one file each, named <node>-<round>
answers/        answers already given
state.json      the run's status: done, waiting, stopped or failed, plus problems found by checks
files/          the nodes' working directory; file protocol paths are relative to it
lock            present while a process is running this graph, and says which process
```

A graph from a graph file always has the `file` protocol, with the working directory above; `process` nodes start there by default, and agents can see the images there. The output of `status` has `problems`, listing each output that failed a check and what came of it, and `used`, the usage so far.

A run directory is run by only one process at a time. A second process trying to `run` or `resume` is refused and told which process on which machine has held it since when; `status` and `answer` are not affected. If the holding process has died (which can be checked on the same machine), the lock is taken over automatically; on another machine this cannot be checked, and a person has to confirm and delete the `lock` file.

`human` nodes in the graph that have no function of their own are answered through the run directory: while there is no answer, the run stops in `waiting` and the process exits, so no process hangs waiting for a person; `answer` writes the answer, `resume` continues, and nodes that finished are not rerun.

Models are named `provider:model`, and keys are read only from environment variables, never written into graph files, run directories or journals: `anthropic:` uses `ANTHROPIC_API_KEY` (and optionally `ANTHROPIC_BASE_URL`), `openai:` uses `OPENAI_API_KEY` and `OPENAI_BASE_URL` (any compatible API), and `fake:echo` calls no service and returns what it receives, for trying out how a graph is wired.

In Python, `interlacing.runs.Run` does the same: `Run.create(dir, description, allow=...)`, `start(inputs)`, `resume()`, `questions()`, `answer(value)`, `status()`.

## MCP server

`interlacing serve --runs dir [--allow module]... [--processes] [--workers N] [--time-limit seconds] [--files dir] [--tools dir] [--apps dir] [--remote address]... [--budget name=number]...` offers the same operations to other applications (usually the model inside that application) through the Model Context Protocol. The transport is stdio, one JSON-RPC message per line. In an application that supports MCP, configure it like this:

```json
{"mcpServers": {"interlacing": {"command": "interlacing", "args": ["serve", "--runs", "/path/to/runs", "--allow", "mypkg"]}}}
```

| Tool | Arguments | Purpose |
|---|---|---|
| `guide` | none | The graph format, and the functions this server lets a graph name (with signatures); a client reads it before writing a graph |
| `check` | `graph` | Build the graph without running it; returns the digest, nodes, inputs and outputs |
| `start` | `graph`, `inputs`, `wait` | Start a run and return its name and status. With `wait: false` it returns `running` at once and the run goes on in the server |
| `status` | `run` | The run's status and open questions |
| `resume` | `run`, `wait` | Continue from the journal; `wait` as above |
| `stop` | `run` | Stop a run in progress; it can be resumed later |
| `control` | `run`, `nodes`, `action`, `team` | Pause, resume, stop or restart nodes in it; while the run is going on (in this server or another process) it takes effect within moments |
| `answer` | `run`, `answer`, optional `question` | Write a human's answer to a question; call `resume` afterwards |
| `runs` | none | All runs the server holds, with their status |

The client is the untrusted party:

- Which modules the graph can reach, whether it may start programs, and how many threads it uses are set on the command line by whoever starts the server. Anything extra the client writes in a call has no effect, and by default nothing can be reached.
- The server decides where runs are kept and generates their names; a name from the client is only looked up under the `--runs` directory, and a name containing a path never matches.
- `{"file": ...}` in a graph can read only files under the `--files` directory; without that argument no file can be read.
- Tool files can come only from the `--tools` directory, and applications used as nodes can only be folders under the `--apps` directory.
- Model keys come from the server process's environment; the client cannot see them or pass them in.
- What nodes print goes to standard error; standard output carries only protocol messages.

Each run is a run directory under `--runs`, the same kind the command line uses, so a run the server started can be inspected and continued from a terminal with `status`, `answer` and `resume`.

Graphs sent by the client are treated as untrusted: nodes cannot give themselves more time than `--time-limit`, and blocks cannot ask for more threads than `--workers` (`Run.create(..., strict=True)`).

Tool calls are answered in parallel, so a long run does not block questions about other runs. By default `start` and `resume` last until the run finishes, needs a human's answer, or fails; with `wait: false` they return at once and the run goes on in the server, queried with `status` and ended early with `stop`. When a human in the graph needs to answer, `start` or `resume` returns `waiting` with the questions; the client takes them to a person, and once it has the answer calls `answer` and then `resume`. The server cannot tell whether an answer really came from a person; the application that connects to it has to ensure that.

## Examples

[examples/data/](../examples/data/) holds graphs written as JSON and the functions they use, run from the command line. `release.json`: draft, wait for an editor's approval, publish. `scouts.json`: a coordinator and a `Pool` of ten places, with default settings in `defaults` and the members' prompt from `prompts/scout.md`; given three directions, only three members run.

[examples/graphs/](../examples/graphs/) has nineteen examples that run as they are: a graph written with plain functions only, a lead with two resident agents, a lead that hires a team during the run, a rough estimate sent first and corrected later, a reviewer rejecting a plan, one of several estimates rejected by name together with a correction of the note sent with it, a node kept running in another process, a graph that adds nodes to itself during the run, a folder ([examples/apps/word_count](../examples/apps/word_count/)) as one node, a worker working freely in a workspace with a tool pool, a mail desk at the door of a block, an external program in a chain, a write-and-review loop, a tree that needs a human's approval, camera to robot arm, passing files by path and fixing a missing file once, a design made into a block and combined, writing a graph for the task during the run and running it, and a model doing spatial planning while a controller executes. Three fuller cases (a physical agent, a modeling assistant, parallel exploration with a verifier) are in [CASES.md](CASES.md).

## Limits

- Resuming is "at least once": an interrupted node is rerun. `ctx.once` keeps things done outside the graph from being repeated, except in the instant between "done" and "recorded", which is covered by handing `ctx.key` to the outside service for deduplication.
- Streams and calls do not go through the transport interface yet; only the in-process implementation exists. Remote nodes (`RemoteNode`) can send only plain values, cannot take streams or calls, and cannot call nodes in the graph from the remote side.
- Device nodes have been tested only with fake devices. Each of the two model APIs was checked once, very briefly, against a real compatible service (plain answers, streaming, tool calls in a graph, JSON output, a wrong key, running a chain from the command line); automated tests still use only scripted fake models, and thinking content, image input and rate-limit retries have not been checked against a real service.
- The run directory lock exists only at the level of `Run`, the command line and the MCP server; when using `Engine` and `Journal` directly, one journal file must not be used by two engines at once. On Windows, locks left by dead processes are not taken over automatically.
- Tests run on Linux, macOS and Windows. On Windows, `\r\n` in external programs' output becomes `\n`; when the console is not UTF-8, non-ASCII characters in the command line's JSON output are escaped.
- The budget is checked at the moment a node reports usage, so it can be exceeded by one call's worth; nodes that report no usage are not bound by it.
- The MCP server has only the stdio transport. Tool calls are answered in parallel; a client cancelling a call (`notifications/cancelled`) stops the run that call was waiting on; when the client disconnects, runs still in progress are stopped at a point where they can continue. It has been exercised in tests by the reference client (the `mcp` package), but not yet connected to a desktop application.
- A synchronous node that times out in the engine's own process is only abandoned: threads cannot be killed, so it ends only when it returns by itself; coroutine nodes are cancelled. `ProcessNode` and `IsolatedNode` are exceptions: as soon as the run stops (including on a timeout), their processes are killed. Long-running nodes you write should check `ctx.stopping()` regularly, or go into an `IsolatedNode`.
- `Pool` places are not reclaimed within a run (see "Choosing a way to put a graph inside a graph" for why). Nodes added during the run with `ctx.change` are rebuilt from their descriptions by `rebuild` on resume, so they cannot be blocks or `GraphNode`s.
- Checks happen only when a payload is sent and when it arrives; a file changed between the two checks is noticed only at the next arrival. With `hash=False`, only sizes are compared.
- When the engine reruns an activation, what the node did outside the graph in the earlier attempt (files written, tools called) is not undone.
- Once the run inside a block is journaled, edges inside the block can carry only plain data.
- A graph run by `SpawnNode` is gone once that activation ends, and cannot be sent more messages in later rounds; for members that must stay, use `Pool`.
- A `rerun` correction reruns the node's `run`: state the node keeps itself (an accumulated count, for example) is not rolled back first, so nodes with such state should use `tell` or `ignore`.
- A failing node fails the whole run, unless its manager `oversees` and is running (see "Member failures go to the manager"). Failures handed to a manager are not journaled: when the run is resumed, the failed activation is rerun.
- When a member that is still working is dismissed, the engine waits at most 5 seconds for it to stop before taking it out of the graph; one that has not stopped is taken out again by its manager before the next model call. If the manager ends before that, the member stays in the graph as stopped and does nothing more.
- `max_nodes` limits the size of a graph, not its cost; the total number of model calls is bounded by `max_runs`, each method's `max_rounds` and the time limits together.
