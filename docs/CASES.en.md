# Three worked examples

**English** · [中文](CASES.md) · [Graph reference](GRAPH.en.md) · [Project overview](../README.md)

Each example starts with the application, maps its responsibilities onto nodes and edges, and identifies the work the application supplies. The examples were exercised with fake models. The first includes a complete runnable program. The kernel delivers information, checks outputs, accounts for usage and supports observation, time limits and recovery.

## Spatial planning with a model and a controller

### The application

The [Q0-Preview README and directory layout](https://github.com/XPolicyLab/XPolicyLab/tree/main/policy/Q0_Preview) describe a system in which an evaluator supplies observations to an adapter. A turn manager consults a model API and passes validated action groups to a motion planner and executor. The executor returns joint actions to the evaluator. This mapping uses the published description and directory layout.

The model describes a gripper pose with world-coordinate points. It can use Python tools for geometry. A plan contains one or two action groups, with a visual check between groups to decide whether to continue or replan. An accepted plan carries a short memory into the next round. The system budgets plans, episode time, turn time and checkpoint time, and archives requests and tool artifacts.

### Mapping to Interlacing

| Application concern | Implementation |
|---|---|
| Observe, plan, act and observe again | `Loop(observe, planning, act, until=at_goal, max_rounds=max_plans)` |
| Model API and tool loop | Use an `AgentNode` with ordinary nodes at the other end of its call edges. |
| Python geometry tool | Use a `ProcessNode` for a separate process or a `FunctionNode` for an in-process function. Connect it with a `call` edge. |
| Camera images | Write them in the workspace. Pass relative paths with the `file` protocol so `AgentNode` can supply images to the model. |
| Validate an action group | Declare `check=valid_plan, repairs=2` on the output port. An invalid plan returns to the model with the reason. The controller receives only accepted plans. |
| Visual check between groups | Connect another `AgentNode` and a `Branch`. Continue on the yes branch or follow a back edge to replan. |
| Bound conversation memory | `AgentNode(memory=...)` takes a function that chooses which messages to retain for each request. |
| Limit plans and reported execution time | Set `Engine(budget={"plans": 100, "seconds": 10800})`. The executor reports usage with `ctx.use(plans=1, seconds=elapsed)`. |
| Limit turn and checkpoint duration | Set the engine's `time_limit` and a node-specific limit such as `time_limit = 60` for the check. |
| Archive a turn | The journal records activation inputs and outputs. An `observe` callback receives events as they happen. Files stay in the run's `files/` directory. |
| Motion planning, collision checks and 25 Hz joint control | Implement these inside the execution node. |

The [runnable planner and arm example](../examples/graphs/planner_and_arm.py) uses substitutes for the robot, model and motion planner. This excerpt shows how the graph connects them.

```python
planner = AgentNode("plan", model, json=True, memory=recent, max_steps=8,
                    system="Give the gripper pose as three world-frame points. Measure before you plan.",
                    tools=[Tool("measure", "Measure the scene from the current images.")],
                    outputs=[Port("plan", dict, check=valid_plan, repairs=2)])
planning = (GraphBuilder().add(planner, FunctionNode("measure", measure, ...))
            .connect("plan.measure", "measure.request", "call")
            .input("in", "plan.task").output("out", "plan.plan").compile(DEFAULT))

graph = Loop(observe, GraphNode("planning", planning, DEFAULT), act,
             until=lambda seen: at_goal(seen), max_rounds=10).build(DEFAULT)
Engine(graph, DEFAULT, time_limit=60, budget={"plans": 10, "seconds": 600}).run({"in": "start"})
```

The planner calls the measurement tool first. Its initial plan puts two reference points 200 mm apart, outside the accepted range of 5–85 mm. The validation error goes back to the model. The revised plan picks up the block. In the second round two action groups place the block on its mark.

### Execution boundaries

1. Keep the 25 Hz joint-control loop inside the executor. The planning graph operates on the scale of seconds. A measured kernel dispatch of about 11 microseconds per envelope is a scheduling observation, not a real-time guarantee.
2. The evaluator drives the step loop. An adapter can run the graph in a separate thread, read observations through a `SourceNode` and send action groups to a queue through a `SinkNode`. At each step it returns the appropriate joint action, holding position until another plan is available.
3. The image-input path was exercised with a fake model. Physical hardware and a real multimodal model still require validation.

## A modeling assistant with a solver, a critic and human review

### The application

A coordinator assigns a series of questions. Each question has a lead who iterates with a solver and a critic until the answer has a proof. A human can intervene and the completed answers feed a paper. In the motivating workflow an optimization result needs an exact value or tight proven bounds. Send each lead the problem file and specify the context that the question requires. The coordinator sets the goal and leaves the delivery format open.

### Mapping to Interlacing

| Requirement | Implementation |
|---|---|
| Repeat a design for each question | Put the solver and critic in a `Block`. Each instance keeps its own conversation. |
| Iterate until the critic accepts | `Loop(solve, critic, until=accepted, max_rounds=4)`. |
| Require a proof | Put a `check` on the solution port. Use `invalid` and `repairs` to choose failure, warning or repair. |
| Verify with a program | Follow the solver with a checker. Wrap the program in a function node to return a failed proof as feedback. A nonzero exit from a plain `ProcessNode` fails the run. |
| Solve in sequence | Use `seq(q1, q2, q3)` or let the coordinator dispatch one question at a time. |
| Pass the problem file | Send a relative path over a `file` edge. Prompts can specify which directories each role should read. Enforce OS file permissions in the host application. |
| Manage a variable number of leads | Use `Tree` or `Pool`. A pool materializes reserved slots on demand. Use `SpawnNode` when the structure itself is decided at runtime. |
| Human intervention | With `Governed`, the coordinator asks through an `approve` tool. A directory-backed run can exit while waiting and resume after the answer. |
| Select a model per role | Set the default model under `defaults`. Override `model` on individual nodes. |
| Replace prompts | Load prompt text with `"system": {"file": "prompts/devil.md"}`. |
| Control cost | Set `--budget tokens=...`. A run that exceeds the budget stops and can resume with a larger budget. |
| Recover after restart | Reopen the run directory. Agent conversations resume from snapshots in the journal. |

This single-question design was exercised with a fake model:

```python
question = Block(lambda: Loop(
    AgentNode("solve", model, json=True,
              outputs=[Port("solution", dict, check=needs_proof, invalid="warn", repairs=0)]),
    AgentNode("devil", model, json=True, system=DEVIL_PROMPT),
    until=lambda verdict: verdict["accepted"], max_rounds=4), name="question")

study = seq(question, question, question, name="study")
graph = Chain(intake, study("study"), paper).build(DEFAULT)
```

The solver's first answer, 111.6, lacked a proof. The port check recorded the node and round in `Result.problems`. The critic sent the answer back. The next answer supplied 110.97 and a proof, allowing the workflow to advance. In a graph file, defaults keep the description short:

```json
{"defaults": {"agent": {"model": "openai:some-model", "time_limit": 900}},
 "method": "governed", "max_rounds": 6,
 "root": {"id": "lead", "kind": "agent", "system": {"file": "prompts/lead.md"}, "firing": "any",
          "inputs": ["task", {"name": "reply", "required": false}], "outputs": ["out", "send"],
          "tools": [{"name": "approve", "description": "Ask the operator before going on."}]},
 "children": [{"id": "q", "kind": "block", "file": "blocks/question.json", "times": 3}],
 "operator": {"id": "operator", "kind": "human"}}
```

### Application responsibilities

The application supplies its UI, chat and formula rendering, paper templates, export and results tables. A local UI can use `Run` and `observe`, or integrate through MCP. For a shared server, the collaboration service assigns tasks and accepts answers. Managers can subscribe to root-run progress over HTTP. [Multi-user review](POSTGRES.en.md)

For a question that should do as well as asking the model directly, inspect what each role actually receives. The graph sets the system prompt, conversation history and tool descriptions. The kernel labels inputs by port when there are several, and supplies validation feedback when an output fails a check. Answer quality still depends on the model and the question design.

## Parallel mathematical exploration with a Lean verifier

Give a lead several exploration slots. Each explorer writes a proof, runs a verifier and uses its diagnostics for the next attempt:

```python
def lean(proof):
    done = subprocess.run(["lean", "--stdin"], input=proof, capture_output=True, text=True, timeout=300)
    return {"ok": done.returncode == 0, "proof": proof, "errors": done.stdout + done.stderr}

def explorer(id):
    body = Loop(AgentNode("prove", model, system=EXPLORE_PROMPT),
                FunctionNode("lean", lean, inputs=[Port("proof", str)], outputs=[Port("verdict", dict)]),
                until=lambda verdict: verdict["ok"], max_rounds=5)
    return GraphNode(id, body.build(DEFAULT), DEFAULT)

graph = Pool(lead, explorer, size=10, max_rounds=1).build(DEFAULT)
Engine(graph, DEFAULT, workers=4, budget={"tokens": 2_000_000}, journal=Journal("run.jsonl")).run({"in": problem})
```

- A message such as `{"to": "induction", "task": "..."}` activates one explorer. Three directions use three of the ten available slots.
- With `workers=4`, explorers can run concurrently. Each iterates inside its own subgraph.
- The loop's `until` stops on successful verification and returns a message such as `{"from": "induction", "reply": ...}` to the lead. If five rounds elapse, the lead gets the last result, including its failure status.
- Pass proof-file paths via the `file` protocol. The lead node combines the proofs.
- On recovery, completed explorers retain their results. An interrupted explorer resumes from the journal in its block.

With a fake model and fake verifier, each of three directions passed on its second attempt. The lead received three results after six model calls. A real Lean installation was outside this exercise. The function wrapper lets the graph treat a nonzero verifier exit as proof feedback.

## Integrate with an existing application

All three graphs can be described in JSON. Run `interlacing serve --runs DIRECTORY --allow MODULE --budget tokens=...` to expose MCP tools for checking, starting, inspecting, answering and resuming runs. A model in another application can author and run graphs through those tools. The host configures allowed capabilities, budgets and activation limits when starting the service.

## Kernel and application responsibilities

| The kernel manages | The application supplies |
|---|---|
| The route and protocol for each delivery | Model behavior and prompts |
| Validation and the response to an invalid output | The business rule used by the validator |
| Reported usage, elapsed time and limits | The monetary value of a call |
| Where graph execution resumes | Compensation or deduplication for external effects, using a key such as `ctx.key` |
| Which node is awaiting an answer | How to present the question |
| Functions, models and programs available to a graph | Isolation of trusted or untrusted code at the host level |
