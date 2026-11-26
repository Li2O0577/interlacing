# Changelog

Versions before 1.0 may change any interface; each change is listed here. From 1.0 on, the names exported
by `interlacing` and the formats of graph files, run directories and journals change incompatibly only
with a new major version.

## Unreleased

- Keep local JSONL runs as the default. SQLite run directories can opt into durable delivery, inspect earlier events and fork an independent run.
- Use PostgreSQL for project membership, task assignment and versioned human approvals. Workers can execute independent root branches in parallel and reclaim interrupted runs. Durable value and artifact edges support challenges, explanations and corrections.
- Fork a settled position in a static root graph and revise a committed JSON output. The fork records the reason and message version. Downstream work follows the graph's correction policies, while the parent retains its original results and approvals.
- Subscribe to root run progress over HTTP with `Client.stream_progress()`. Resume from a recorded event number and report business stages with `ctx.progress()`.
- Configure synchronous function retries for connection errors, timeouts or both. Worker takeover preserves the remaining attempts. A supervisor can authorize another budget after exhaustion.

See the [service and migration guide](docs/POSTGRES.md) and [branch verification record](docs/BRANCH_REVIEW.md).

## 0.16.2

- Inspect recorded dependencies, waits and revisions with `interlacing show` and the MCP `show` tool.
  The CLI also exports Mermaid and DOT diagrams.
- On resume, ordinary value corrections can rerun downstream work and update the existing report in place.
- Recover run directories after the owning process exits unexpectedly on Windows.
- Add bilingual Python handbooks, a simulation review example and reuse text digests during fan-out.

## 0.16.0

- Security: `Catalog.function` lets through only functions defined in an allowed module. A name that reaches
  through an allowed module into one it imported (`mypkg.os.system`), a name with a `__` part, and a function
  imported into an allowed module from elsewhere are refused. Before, a graph written by a model, an MCP
  client or a `SpawnNode` request could run any function the allowed module had imported.
- The graph format is told: `interlacing.spec.GUIDE` (one page, with examples), `functions(catalog)` (the
  functions a catalog allows, with signatures and first doc lines) and `guide(catalog)` (both). The MCP server
  has a `guide` tool, the command line has `interlacing guide [--allow MODULE]...`, and `SpawnNode.tool(name=
  "grow")` gives an agent the node as a tool whose description carries both.
- Graph files are written as Python is: a `function` node reads its inputs, its output `out` and its context
  from the function when they are not listed; a node of a chain, loop or fan may be a method written in place.
- A part of a description in the wrong shape (an edge, a list of nodes, ports, inputs, outputs, `manages`) is
  refused with a `SpecError` that shows the right shape, never a `TypeError` or `AttributeError`. A wrong port
  or graph input is answered with the ones there are.
- An agent paused while its model was answering holds the answer until it is resumed; stopped, it does not send
  it.
- A journaled run whose agent hires needs no `rebuild=`: a hired member carries `made_by` in its description,
  and on resume the agent that hired it makes it again from the same template.
- `GUIDE` lists the ports of the common kinds of node (`human`, `branch`, `until`, `pass`, `collect`, `spawn`,
  `source`, `sink`); a test checks them against the nodes. An object with `__call__` whose class is defined in an
  allowed module may be named.
- When everything left in a run is paused, a node waiting on it (a lead waiting for a paused member) ends with
  the run instead of being started again at once, which asked its model again and again and, with a fast model,
  never ended.
- `Run.answer` (and the `answer` command and tool) says which question it answered and that the run is to be
  resumed. `interlacing --help` gives the exit codes, `resume --help` says it keeps what the run was started
  with, and `guide` without `--allow` says how to name modules.
- The command line puts the directory it is run in on the module search path, so `--allow` can name a module
  beside it, and output closed early (`| head`) is no longer an error.
- The README has a banner image (`docs/images/interlacing.jpg`).
- `docs/GRAPH.en.md` is an English version of the full description; the two versions link to each other.
- `docs/GRAPH.md`: a duplicated `artifact` row and a broken `Catalog` table were fixed, and the resident agent's
  tool list now names `fail`.
- `tests/test_scenario.py` runs one team through a member failure, a rejection, a correction and a resume from
  the journal, on 0, 1 and 4 workers.

## 0.15.0

- `ctx.receive(ports=None, timeout=None)` (and `await ctx.areceive(...)`): a running node waits for its next
  message and takes it, in the order messages came. While it waits it holds none of the run's workers, so
  `workers=0` or `1` cannot be blocked by it, and its time limit does not run. It gets None when the time is
  up, when it is stopped or the run is ending, or when nothing more can come because every other activation
  has ended or waits too: two nodes waiting for each other both get None and can finish. What it takes is
  the activation's, journaled with it and put back if the activation is cut. Observers are told
  `received`. A node told to end no longer sends what its `run` returns.
- `ctx.commit()`: a long activation saves its progress (what it took, sent and holds) to the journal and goes
  on. Taken up after the process ended, or switched on again after a stop, it is not run again from the start:
  the node gets back its saved state and runs once more with `ctx.resumed` true and no inputs. A restart
  starts it again from what it first had.
- Resident agents: `AgentNode(..., resident=True)` (`"resident": true` in a graph file) stays for one
  activation until it says it has finished. It sends each output when it is ready (`send_<port>`, even with
  one output), waits for messages (`wait`, holding no worker, and told when nothing more can come), keeps a
  task board (a line per output, items it adds with `plan` and ticks off with `done`, its last `note`), and
  ends with `finish`, which asks once when an output was never sent. Its progress is saved before each wait
  and with each note; taken up again it goes on with its conversation and its board.
- Plain functions as an agent's tools: `AgentNode(tools=[function, ...])` or `Tool.of(function)`. The name,
  description and JSON schema of the arguments come from the function; a `ctx` parameter gets the agent's
  context. The agent calls it itself: no node, no edge. Graph files name one with `{"function": "module.name"}`
  under `allow`. `time_left` tells an agent how long it has before its time limit (`ctx.time_left()`).
- Corrections: `ctx.emit` returns the message's id (`"out#3"`), and `ctx.correct(id, payload)` puts `payload`
  in its place. Not yet read, the message is replaced. Read, each receiver does what its `on_correction`
  says: `rerun` (the default) runs the activation that read it again with the corrected value, and what that
  sends corrects what it sent the first time, so corrections go on downstream; `tell` (an `AgentNode`'s
  default) delivers it as a new message, for which `ctx.replaces(port)` gives the id it corrects; `ignore`.
  A graph output it reached is changed in place. Corrections are checked like any message, count against
  an edge's `max_rounds`, are journaled and kept when a run is taken up; the last one counts. Envelopes have
  `replaces`; graph files may give a node `on_correction`. Observers are told `corrected`.
- Resident agents: `send_<port>` answers with the id of what was sent, a `correct(id, content)` tool corrects
  it, the task board counts corrections, and a message that corrects one read before says so.
- Rejections: an edge may take them, `connect(a, b, challenges=N)` (`"challenges"` in a graph file), N in all.
  Its receiver calls `ctx.reject(port, reason)` (or `await ctx.areject(...)`) on what it took last there, and
  waits, holding no worker, for the sender's answer: explained (the value stands), corrected (the new value;
  the correction is the answer and nothing else follows for this receiver) or refused (none left, the sender
  is stopped or gone, or no answer can come; a sender stopped or taken out after it was rejected, even while
  it answers, refuses at once). An activation run again after a correction may reject what it reads. A sender
  waiting in `ctx.receive` is woken with an empty mapping and takes them up with `ctx.rejections()`, answering
  with `ctx.explain(id, text)` or `ctx.correct`; one left unanswered is answered by `Node.challenged(ctx,
  rejection)` in an activation of its own, which for a program says that its result stands.
  `ctx.challengeable(port)`. Rejections are journaled; observers are told `answered`.
- Rejections with a direction: on a port fed by several edges, `ctx.reject(port, reason, which)` names the
  one it rejects (its sender, its edge or its envelope id; the last one taken when left out), and
  `ctx.taken(port)` lists what the activation took there. A rejection says what else the same activation
  sent (`sent`: output port -> names, up to 32), so the sender can correct those too; corrections go on
  downstream as usual.
- Agents and rejections: on an edge that takes them an agent is told to judge what came (a default prompt,
  `DOUBT`, or its own, `doubt=`) and has a `reject` tool. An agent whose message is rejected is shown it and
  answers with `explain` or `correct`; a resident one sees it in `wait`, one that has finished is called back.
  It is shown what else it sent with it, and `correct` with an `id` corrects any of those; the `reject`
  tool takes `which`, and an agent is told who sent each message on a port fed by several.
- A served node (`RemoteNode`) corrects what it sent: its `ctx.emit` returns its own name for the message,
  which its `ctx.correct` takes.
- Hiring: a resident agent may build its own team when it is allowed to, `AgentNode(hire=N, templates={...},
  hire_rounds=10)` (`"hire"`, `"templates"` in a graph file, naming `Catalog(templates=...)` or giving
  descriptions). N is how many in the whole run; without it there are no hiring tools. Tools: `hire`
  (a member from a template, managed by the agent, with `to_<name>` and `from_<name>` lines), `dismiss` (a
  member still working is stopped at once and taken out of the graph once it has ended; its name is free then),
  `control`, `team`, `connect` (within its team). A member that may hire gets no more than its hirer; one
  made from a description works in a directory inside its hirer's (`layer`).
- A manager may oversee (`Node.oversees`, `"oversees": true` in a graph file; an agent that may hire does):
  when a node it manages fails while it runs, the run goes on, the member is stopped with its inputs kept,
  and the failure comes to the manager (`ctx.failures()`, waking `ctx.receive`), which restarts it, takes it
  out or fails. A failure it never takes up fails the run as before. Agents see `[failed]` in `wait` and
  have a `fail(reason)` tool.
- `Change.ports(node, inputs, outputs)` gives an existing node more ports while the run goes on
  (`Node.add_ports`, taken back if the change is refused), and `Change.manage(manager, *members)` gives
  nodes added while it runs a manager. `ctx.team()` says how the nodes a node manages stand.
- Nodes of a graph a node runs inside itself are switched by path, `engine.control("box/x", ...)`, by
  whoever may switch the node.
- Examples: `resident_team.py`, a lead and two resident members; `corrected_estimate.py`, a quick estimate
  corrected once those who read it are done; `rejected_answer.py`, a reviewer rejects a plan and the planner
  is called back to correct it; `hired_team.py`, a lead hires two members, gives them work and dismisses them;
  `directed_rejection.py`, a merge rejects one of two estimates by its sender, which corrects the note it sent
  with it too.

## 0.14.0

- Shorter ways to write a graph. A plain function is a node: its name is the id, its parameters the inputs,
  its annotations the schemas, and what it returns leaves through `out` (`as_node`, `FunctionNode.of`).
  `@node(...)` writes only what differs. `Chain`, `Loop`, `Fan` and `Tree` take functions and other methods
  where they take nodes. `interlacing.run(thing, inputs)` builds and runs in one step.
- Protocols may be left out: a compiled graph keeps the protocols it was built with, and `build()`,
  `compile()`, `Engine(graph)`, `GraphNode(id, graph)` and `ctx.engine(graph)` use them, or `DEFAULT`.
  `connect(a, b)` is a `value` edge.
- A port that says nothing of what it produces may feed a port that asks for a schema; what arrives is
  checked there, and fails or warns as the receiving port says. Ports whose schemas disagree are still refused.
- `FolderNode` (`"kind": "folder"`): a whole application in a folder, described by its `node.json`, as one
  node. Inputs go in as one JSON object and the answer comes back as one, by file or on standard output; a
  text format suits plain command-line programs. Graph files reach only the folders under `--apps`
  (`Catalog(apps=)`, `Run.create(apps=)`, the server).
- A graph changes while it runs through one explicit operation: `ctx.change(Change().add(...).connect(...)
  .remove(...).disconnect(...).output(...))`. The change is checked by every rule a graph is built by and is
  refused whole (`GraphError`) or made at once; observers are told (`changed`); with a journal it is kept with
  the activation and made again on resume, the added nodes rebuilt from their descriptions
  (`Engine(rebuild=)`, done by `Run` itself; `spec.load_node`). `Engine(max_nodes=)` bounds the growth, and
  `Engine.graph` is the graph as it stands.
- `RemoteNode` (`"kind": "remote"`): a node that lives in another process or on another machine and stays
  up between activations. It is served there with `serve_node(node)` or `interlacing node-serve
  module:name`; one JSON object per line over TCP carries the inputs, what the node emits, what it uses and a
  stop. A server listening beyond 127.0.0.1 needs a token; graph files read the token from an environment
  variable (`token_env`) and reach only the addresses allowed (`Catalog(remotes=)`, `Run.create(remotes=)`,
  `--remote` on `check`, `run` and `serve`).
- Nodes may be coroutines: `run` (and `open`, `close`) may be `async def`, and so may the function of a
  `FunctionNode` or `@node`. A graph that has such nodes runs on an event loop: they are awaited there, without
  a thread each, and the others go to threads; `workers` still bounds how many activations are under way,
  `workers=0` still runs one at a time in a fixed order. One that overruns its time is cancelled. `await
  ctx.acall(port, payload)` calls from such a node. `arun`, `aresume` and `ago` run such a graph on the
  caller's loop; `run` starts a loop of its own. `Node.asynchronous` says which kind a node is.
- A switch on every node: `engine.control(nodes, "pause" | "resume" | "stop" | "restart", team=False)`, one
  node or several. A running node acts on it at its next step (`ctx.checkpoint()`, called by `AgentNode`
  before each request to its model and each tool): paused, it waits there; stopped, it ends and gives back
  what it took; restarted, it also forgets what it held (`Node.reset`). Who manages whom is part of the graph
  (`GraphBuilder.manage`, `"manages"` in a graph file, `Graph.team`), and `ctx.control` switches only nodes
  under the caller. Switches are journaled and kept on resume; `Run.control`, `interlacing control` and the
  MCP tool `control` set them, between goes too. A run whose remaining work is all paused ends, not complete,
  with `Result.held`, to be taken up later. Time spent paused does not count against a time limit.
  A run going in another process is switched through its directory (`controls/`), and `changed` names only
  the nodes that changed. Pausing reaches work handed outside: a `RemoteNode`'s far side waits at its own
  next step, and a program a node runs is suspended with it (on POSIX), its own timeout not counting the
  pause. `ctx.paused()` tells a node it is paused. `Engine.going` says whether a run is under way.
- The engine is split into parts that each do one thing (`wiring`, `scheduler`, `checks`, `ledger`,
  `recorder`, `clock`, `context`, `result`). Nothing exported changed, and it is as fast as before.
  `Engine.used` says what the run has used so far.

## 0.13.0

- `Workspace` and `Area`: a run's files laid out by node (`<node>/`, `shared/`), one more layer for each block;
  each node reads and writes within its bounds (`"reads"`, `"writes"` in a graph file); who wrote each file is
  recorded. `Run.files()` and `interlacing files --dir RUN`.
- `WorkerNode` (`"kind": "worker"`): an agent with file tools for its area and a tool that runs commands in its
  own directory; standard inputs and outputs.
- `ToolPool`: a directory of tool files (a Python function, or a JSON file naming a program). Agents take tools
  by path; each use runs in a process of its own. `Catalog(tools=)` and `--tools DIRECTORY` on the command line
  and the server.
- `Tool(run=...)`: a tool the node answers itself, with no port and no edge.
- An agent with an area may only send paths it can read; a path outside goes back to the model with the reason.

## 0.12.0

- `await engine.arun()`, `aresume()` and `ago()`; cancelling the wait stops the run. A `FunctionNode`'s function
  may be `async def`.
- `ctx.once(name, do)`: what a node does outside the graph is kept in the journal and is not done again when
  its activation runs again.
- `IsolatedNode`: a function in a process of its own, killed when it overruns or the run stops (`"kind":
  "isolated"` in a graph file, where processes are allowed).
- The MCP server answers tool calls side by side. `start` and `resume` take `wait: false`; a new tool, `stop`;
  a cancelled call stops its run; a client that goes away leaves nothing running. Tried with the reference
  client.
- `Run.status()` says `running` while a process holds the run directory; `Run.stop()`.
- The model interfaces are tested against a stand-in service. Fixed: a `Retry-After` of 0 became a wait of five
  seconds; an agent did not give a model its thinking back between tool calls, so it stopped thinking there.

## 0.11.0

First release on the package index.

- Nodes (`FunctionNode`, `AgentNode`, `ProcessNode`, `SourceNode`, `SinkNode`, `HumanNode`, `GraphNode`),
  protocols (`value`, `latest`, `state`, `signal`, `stream`, `call`, `artifact`, `file`) and methods (`Chain`, `Loop`,
  `Fan`, `Tree`, `Star`, `Governed`, `Pool`).
- The engine: workers, journal and `resume`, time limits, budgets, an observer, output checks with repairs.
- `Block`, `seq`, `par`; graphs as JSON with `Catalog`; `SpawnNode` for graphs written during a run.
- `Desk`: a node that takes in from several places, sorts, and hands out to several; `Fan` joins a split and a
  join to its branches by name, and shows everything they take and give.
- Run directories, the `interlacing` command, and an MCP server over stdio.
- Graphs that are not trusted are held to the run's time limit, node count and workers (`firm`, `strict`).
