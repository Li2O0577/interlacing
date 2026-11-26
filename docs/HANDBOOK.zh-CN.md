# interlacing · Python 使用与维护手册

**中文** · [English](HANDBOOK.en.md) · 需要 Python 3.11+。第一个例子可用已发布的 **0.16.2** 运行。使用本手册中的持久化 SQLite、PostgreSQL 协作和新增 CLI 命令，请在本分支仓库根目录执行 `python -m pip install -e ".[postgres]"`（仅本地运行可用 `-e .`）。第一次使用，先做前两个例子；接手一个已有项目，直接查最后的维护地图。

[快速开始](#start) · [建图与顺序](#graph) · [协议选择](#edges) · [节点选择](#nodes) · [组合与动态结构](#compose) · [运行与恢复](#run) · [质疑与更正](#review) · [JSON、CLI 与 MCP](#cli) · [维护与排错](#maintain)

<a id="start"></a>

## 1 · 跑通一条数据流

一个实验产生三个读数。我们要计算读数的跨度，并把它写成一句话。把下面的 Python 代码保存为脚本执行即可；本节全部在本地运行。安装发布版：

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

读这段代码时注意三个名字。单独运行 `spread` 时输入名是它的参数 `samples`；`Chain` 将首尾包装成图级的 `in` 和 `out`。一次运行可以向同一出口发多条消息，所以 `outputs["out"]` 是列表，`last("out")` 取最后一条。一次**激活**是某个节点拿到一批输入后的一次执行；一次**运行**可以包含很多次激活。

<a id="graph"></a>

## 2 · 把“先检查，再写报告”写进图

同一份数据要送到检查节点和报告节点；报告还要等一个放行事件。这里手工连边比写成一条链更清楚。下面是可独立执行的完整脚本。

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

`Node` 提供计算，`Port` 给输入和输出命名并声明形状，`Edge` 把两个端口连起来，`GraphBuilder.compile()` 检查接线并生成 `Graph`。默认的 `Firing.ALL` 等待必需输入；`Firing.ANY` 用于任一触发输入到来即可工作的节点。例子里 `approved` 只表示放行事件，收到的内容是 `None`。若需要携带同意或拒绝的布尔值，应改用 `value` 并在接收端检查。事件顺序由依赖关系决定；增加 worker 可让已就绪的节点同时工作。

**输出契约。** `Port("score", float, check=validate, repairs=1)` 先检查类型，再执行适用的协议和业务检查。业务检查返回原因字符串可把问题告诉发送者。重试用完时默认失败，`invalid="warn"` 可记录问题并继续。用 `required=False` 指定可选输入，`many=True` 允许多条边接到一个输入。端口检查适合发送时就能判断的错误；需要结合其他结果判断时，见[质疑](#review)。

<a id="edges"></a>

## 3 · 选择边的协议

同一个上游输出可以连到多个下游，各条边分别决定“保留什么、何时唤醒、是否等答复”。相机帧、速度设置和机械臂命令放在同一张图里时，这三个问题的答案各不相同。[可运行示例](../examples/graphs/camera_to_arm.py)

| 协议 | 何时选用 | 接收侧行为 |
|---|---|---|
| `value` | 每份模拟结果都要处理 | 按序排队 |
| `latest` | 只关心最新帧 | 更新未读值，到达可触发 |
| `state` | 随时可读的当前设置 | 保留最新值；由其他输入触发时读取 |
| `signal` | 通知某一步完成 | 收到事件，内容为 `None` |
| `stream` | 一边产生一边消费分块结果 | 在发送端本次激活还在写入时即可读取 |
| `call` | 模型或程序要向另一个节点请求答复 | 发送方用 `ctx.call` 等待返回值 |
| `file` | 传工作目录里的已有文件 | 发送相对路径，两端检查；用 `with_files(DEFAULT, root)` |
| `artifact` | 大内容需要按摘要引用 | 内容存入 store，信封携带 `Ref`；用 `standard(store)` |

`DEFAULT` 包含表中前六种。`FileOf(".png", max_size=5000000)` 可作为文件端口的约束；`MemoryStore` 和 `DirectoryStore` 从 `interlacing.protocols` 导入。要扩展协议，继承 `Protocol`，在 `Protocols` 中注册；`Delivery` 描述投递方式，`Envelope` 携带消息及其来源。允许反馈循环时显式使用有轮数上限的回边。

<a id="nodes"></a>

## 4 · 选择节点和执行环境

已有 Python 函数可以直接放进方法。端口来自函数签名；要覆盖输出名、检查或输入约定时用 `@node(...)` 或显式构造 `FunctionNode`。要在激活中途发消息、答复质疑、保存状态或开关设备，再考虑 `Context` 和自定义 `Node`。用下表按执行地点选。

| 想接入的对象 | 入口与用法 |
|---|---|
| 本地函数或有状态的 Python 对象 | `as_node(fn)` 适配函数；`FunctionNode(..., context=True)` 给函数传入本次激活的上下文。继承 `Node` 可实现 `open/run/close` 和 `snapshot/restore` |
| 模型 | `AgentNode(id, model, ...)` 接受有 `chat(system, messages, tools, max_tokens)` 方法的提供方。普通函数可直接作为工具；用 `Tool` 和 `call` 边把请求交给另一个节点 |
| 保持会话的模型 | `resident=True` 让同一次激活反复收发消息；任务板记录计划，`wait` 等消息，`finish` 结束。配置 `hire` 和模板后才可招募成员。[示例](../examples/graphs/resident_team.py) |
| 命令、应用目录、需要独立进程的函数 | `ProcessNode` 包装命令；`FolderNode` 读取应用的 `node.json` 和输入输出约定；`IsolatedNode` 在可终止的子进程执行函数。[应用目录示例](../examples/graphs/folder_as_node.py) |
| 要跨激活保留的远程服务 | 服务端 `serve_node(node)` 或 CLI `node-serve`；图中用 `RemoteNode` 代理。端口发现和激活依赖服务可达。[示例](../examples/graphs/served_elsewhere.py) |
| 设备、接收端、人 | `SourceNode` 把可迭代读数发到 `out`；`SinkNode` 对每个输入执行写入，可设 `answers=True`；`HumanNode` 让人提供答复 |
| 文件和命令工具 | `Workspace` 划分节点区域 `Area`；`WorkerNode` 给模型区域文件工具和命令工具；`ToolPool` 按路径加载工具。命令继承宿主进程权限。[示例](../examples/graphs/worker_in_workspace.py) |
| 将多路输入分拣成多路输出 | `Desk` 用具名输入输出和分拣函数衔接分支。[示例](../examples/graphs/desks_between_blocks.py) |

**先用脚本模型验证接线。** 下面的 Agent 调用真实的图运行时，模型回复由本地函数给出。接真实服务时，可从 `interlacing.models` 导入 `AnthropicProvider` 或 `OpenAICompatProvider`；`Provider` 是适配接口，`ChatResult` 承载回复和工具调用。

```python
from interlacing import AgentNode, ChatResult, FakeProvider, run

def reply(system, messages, tools):
    return ChatResult(text="ready")

agent = AgentNode("reviewer", FakeProvider(reply))
assert run(agent, {"task": "Check availability"}).last("out") == "ready"
```

<a id="compose"></a>

## 5 · 组合、复用与运行中改图

**方法**提供可重复使用的接线方式。选 `Chain(a, b)` 做顺序加工，选 `Fan(a, b)` 拆分再汇合，选 `Loop(a, b, max_rounds=3)` 做有界反馈。多级协调选 `Tree`，单层中心协调选 `Star`，协调者要经过人工放行选 `Governed`，有名额上限且只在收到任务时启用成员选 `Pool`。方法的 `build()` 返回可交给引擎的图；自定义结构可以实现 `Method.build(protocols)`。[方法及端口约定](GRAPH.md#方法)

| 需求 | 用法与生命周期 |
|---|---|
| 把现成子图当节点 | `GraphNode` 对外暴露子图的输入输出 |
| 一份设计用好几次，各自持有状态 | `Block(factory)` 保存造图配方，每次调用创建独立实例；`seq` 串联、`par` 并联。[示例](../examples/graphs/blocks_joined.py) |
| 更改当前运行的拓扑 | 节点调用 `ctx.change(Change().add(...).connect(...))`；引擎验证整组修改，通过后一起生效。带日志运行时应考虑新增节点如何在恢复时重建。[示例](../examples/graphs/changed_while_running.py) |
| 执行节点刚生成的一张图 | `SpawnNode` 接收图描述和输入，在给定 `Catalog` 和节点数等上限内运行子图；`tool()` 将该入口交给 Agent。[示例](../examples/graphs/grown_at_run_time.py) |

<a id="run"></a>

## 6 · 执行、观测和恢复

| 入口 | 什么时候用 | 要看什么 |
|---|---|---|
| `run(thing, inputs, **options)` | 脚本或测试中直接运行函数、节点、方法或图 | 返回 `Result`；看 `outputs`、`last(name)`、`complete` |
| `Engine(graph, ...).run(inputs)` | 需要指定调度、预算、日志、观察器 | `workers` 控制并行工作线程，`time_limit` 约束单次激活，`budget` 按节点报告的用量累计，`observe` 接收事件；协程入口为 `await engine.arun(inputs)` |
| `Engine(..., journal=Journal(path)).resume()` | 应用自己管理图对象和日志，进程重启后接续 | 新引擎加载同一图和同一日志；`Journal(path, sync=True)` 会在每次写记录时额外执行磁盘同步 |
| `interlacing.runs.Run` | 多个进程交接同一份运行目录，典型情况是等人 | `Run.create(directory, description, ...)`，然后 `start(inputs)`；后来用 `Run(directory)` 查看、答复或继续 |

**读一次运行。** `Result.rounds` 是各节点的激活次数，`used` 是累计用量，`problems` 记录检查或处理过的问题，`held` 记录结束时仍暂停或停止的节点。失败时检查 `RunError.node`、`used` 和 `problems`。节点可用 `ctx.use(...)` 上报成本，用 `ctx.time_left()` 查看单次激活余下时间；`time_left` 也可作为 Agent 工具。超时的进程内线程可能继续执行外部操作，需要强制终止的任务应选择独立进程。

**长任务的三个停点。** ① `ctx.receive()` 在激活中等下一条消息。② `ctx.checkpoint()` 给正在运行的节点一个响应暂停或停止的机会。③ `ctx.commit()` 将本次进度记入日志；自定义有状态节点用 `snapshot/restore` 描述可恢复的状态。未完成且未保存的工作可能重做。外部写入用 `ctx.once(name, do)` 复用已记账的结果；如果进程恰好在外部操作完成后、记录前退出，下次仍可能重试，应将稳定的 `ctx.key` 交给支持幂等的外部服务。[恢复测试](../tests/test_commit.py) · [外部操作测试](../tests/test_once.py)

<a id="review"></a>

## 7 · 输出不对时，谁来处理？

| 发生了什么 | 谁做决定 | 0.16.2 的行为 |
|---|---|---|
| A 发出前就发现产物格式不合规 | 输出端口 | `Port("score", check=validate, repairs=1)` 按配置尝试修复、警告或失败 |
| B1 结合其他证据怀疑 A 的模拟结果 | B1 主动提问，A 答复 | 边用 `connect(..., challenges=N)` 允许质疑。B1 调 `ctx.reject(port, reason)` 等答复；多来源时可用 `which` 指定发送者。A 解释或更正，普通节点也能实现 `challenged(ctx, rejection)` |
| A 已经知道哪份消息错了 | A 更正，接收者按策略处理 | `sent = ctx.emit(...)` 后可用 `ctx.correct(sent, value)`。未读的值可替换，已读的按 `rerun/tell/ignore`；恢复后，普通值输入可按 `rerun` 原位修订，旧日志等缺少可恢复历史时按 `tell` 通知 |
| A 想暂停受影响的 B2 | 图作者声明管理关系，运行时执行 | `builder.manage("a", "b2")` 声明权限，A 才能用 `ctx.control("b2", "pause")` 直接暂停它。操作员可通过 `engine.control` 或 CLI 控制节点 |

**实际走一遍普通节点的驳回。** 在仓库根目录执行下面命令。两个估算器分别给出 50 和 76，合并节点点名驳回偏差大的估算。上游把估算改成 74，同时改正一起发出的说明。最终面积是 75，日志节点拿到修订后的文字。这个例子覆盖“质疑哪份结果、谁回答、哪些相关输出一起改”；对受影响节点逐层评估和请求暂停，需要图作者编排。

```sh
python examples/graphs/directed_rejection.py
```

<a id="cli"></a>

## 8 · 用 JSON、CLI 或 MCP 运行同一类图

Python 适合直接传函数；JSON 用可查找的名字描述节点和接线。用 `dump(graph)` 得到描述，用 `load(description, catalog)` 构图；读写文件用 `interlacing.spec.read/write`。可重载的描述需要能从 `Catalog` 解析的节点和函数。用 `interlacing guide --allow MODULE` 查图格式和可用函数，Python 中对应 `interlacing.spec.GUIDE`、`functions(catalog)` 和 `guide(catalog)`。由启动方决定 `--allow`、`--processes`、`--apps`、`--tools` 和 `--remote` 的能力范围。

**练习：人工批准后继续。** 先检出本仓库，在根目录装好 `python -m pip install -e .`，再逐条执行以下命令。使用一个新的运行目录；例子使用 POSIX shell 引号。第一次 `run` 会正常退出并返回 **3**。查看待答问题，写入答复，再显式恢复；最后输出应包含 `published: A note about tides.`。[图文件](../examples/data/release.json) · [对应函数](../examples/data/steps.py)

```sh
cd examples/data
interlacing check release.json --allow steps
interlacing run release.json --dir demo-run --allow steps --input topic=tides
interlacing status --dir demo-run
interlacing show --dir demo-run
interlacing answer --dir demo-run --json '{"approved": true}'
interlacing resume --dir demo-run
```

新建运行时可加 `--journal sqlite`，将执行历史写入 `journal.sqlite3`。运行目录记录后端选择，之后的 `resume`、`show`、`status` 和 `control` 自动沿用。已有运行默认使用 JSONL。图定义、人工问题和回复、工作区文件都保存在运行目录。Python 入口可用 `Run.create(..., journal="sqlite")`，也可将 `SQLiteJournal(path)` 传给 `Engine`。

在 `run` 或 `serve` 上同时使用 `--journal sqlite --durable`，让消息提交后才唤醒接收节点。节点代码执行前，运行时会记录它实际绑定的输入。恢复先加载周期检查点，再处理之后的事件。未完成的节点按原输入重放，输出匹配时复用已提交的投递。重放结果或改图操作不一致时，系统会报错。节点可通过 `ctx.commit()`、`snapshot/restore` 和 `ctx.resumed` 恢复显式进度。Python 入口可用 `SQLiteJournal(path, durable=True)`。

该模式支持普通值、启动时绑定输入、追加式改图，以及暂停、停止与继续。使用内容引用（`artifact`）时，Python 引擎需显式配置含内容存储的协议目录，如 `standard(DirectoryStore(...))`。本地 CLI 和 `Run.create` 的默认协议目录未注册 `artifact`；PostgreSQL 工作进程使用服务配置的内容目录。普通值和内容引用边均支持带次数上限的质疑及答复恢复。流、调用、运行中接收、节点重启和替换图恢复使用标准模式。持久化投递为图内消息去重，未完成节点的代码仍可能重试。外部服务支持幂等键时，应为写操作使用稳定的键。

**修正已记录的消息。** 用 `ctx.correct(token, value)` 修正本节点发出的消息。尚未读到的副本在原邮箱位置替换。读者使用 `on_correction="rerun"` 时，系统用修订后的输入和该轮其他原始输入重新执行该轮，并更新节点重新发出的输出位置。更新可继续触发下游返工。`tell` 将修订作为新输入投递，`ignore` 让读者保留此前结果。重复提交当前值沿用同一版本。日志记录待返工项，重启后继续处理，每次发布前校验本轮绑定的输入版本。历史中保留该轮被替代前已经提交的结果。

历史视图 `show --at` 的 `rework` 列出待返工的原始轮次和投递编号。加 `--timeline` 可查看修订版本、返工绑定及被替代的激活。对于已保存中途进度或执行过改图的激活，`rerun` 策略会把修订作为新输入投递。返工更新重新发出的输出位置，未再次发出的旧输出仍留在记录里。外部副作用沿用节点自身的执行和幂等规则。

**查看历史与创建分叉。** 对已经启动的持久化 SQLite 运行执行以下命令。事件 `1` 是接收初始输入的位置。`checkpoints` 列出最近保留的 16 个恢复快照，任何已提交的事件仍可用 `show --at` 查看，包括图版本、输出值和执行到一半的节点。

```console
$ interlacing checkpoints --dir source-run
$ interlacing show --dir source-run --at 1 --timeline
$ interlacing fork --dir source-run --to variant-run --at 1 --input in=42 --execute
$ interlacing resume --dir variant-run
```

`fork` 需要一个新目录和当时没有未完成激活的事件。可更换该位置尚未消费的图输入，在事件 `1` 也可提供原先遗漏的图输入。分叉有独立日志、运行编号和工作区，并记录父运行及事件。工作区文件和人工问答没有历史快照。省略 `--execute` 时分叉只供查看。显式选择 `--execute` 后，`resume` 会按照继承的运行权限执行节点，节点可以产生外部副作用。Python 调用对应 `Run.checkpoints()`、`Run.show(at=...)` 和 `Run.fork(..., at=..., inputs=..., execute=True)`。

要从中间结论另试一条路线，可在静态根图中选择已稳定的事件，将 `{"node":"quote","token":"price#1","version":1,"value":120,"reason":"报价更新"}` 传入 `Run.fork(..., output=edit)`，或将对应 JSON 交给 `fork --output`。普通前向 value 边沿用原有更正策略。已读过报价的下游可以重新审核，独立工作保留。详见[服务端示例](POSTGRES.md)。

用 `interlacing show --dir demo-run` 可以按输入端口查看已记录的投递。等待审批时，`publish.note` 已收到来自 `draft.note` 的投递，`publish.verdict` 还在等 `editor.answer` 的首次投递记录。命令仅读取保存的图和完整日志行，无需加载节点代码。正在运行时展示的也是已落盘的证据。图中的增删节点与接线会根据已提交的变更重建。

加 `--timeline` 按日志顺序看激活轮次、质疑与答复、节点开关和上报用量。新日志的 `elapsed_seconds` 是本次激活记录片段的墙钟耗时（包括等待）；旧记录没有耗时字段。失败运行另有 `failure`。用 `--format mermaid` 或 `--format dot` 导出节点与端口连线。若外部替换图只留下摘要，输出会标明最后已知拓扑。[模拟结果的两种审查路径](../examples/graphs/simulation_review.py)

`--timeline` 同时返回 `provenance`。可从输出的初始版本和替换版本找到对应记录，再通过 `scope_id` 和 `event_seq` 定位执行、查看它读过的投递及生产记录。子图完成后即使清理恢复日志，`children` 也保留这些元数据。旧日志可继续查看，缺失或无法唯一匹配的投递来源会明确标记。

`run/resume` 的退出码：`0` 完成，`1` 失败或被拒绝，`3` 等人，`4` 已停止。`status` 查运行状态和问题，`files` 查文件归属，`control pause|resume|stop|restart` 操作单个节点或管理树。目录内有 `graph.json`、`options.json`、`inputs.json`、选定的日志、`state.json` 和问题、答复、文件。SQLite 日志旁可能出现 `-wal` 和 `-shm` 文件。继续整次运行用 `resume --dir`，让某个暂停节点恢复用 `control resume NODE --dir`。

**MCP 入口。** 让客户端以 stdio 启动 `interlacing serve --runs /path/to/runs`，按需添加上述能力参数。加 `--journal sqlite` 后，此服务新建的运行使用 SQLite。常用调用顺序是 `guide → check → start → status`；遇到人工问题用 `answer → resume`。另有 `runs`、`stop`、`control`。长任务可在 `start/resume` 传 `wait: false` 后查询状态。要单独提供一个远程节点，使用 `node-serve`。[MCP 工具参数](GRAPH.md#mcp-服务)

**试用 MCP 审查模拟。** 在 `examples/data` 中启动服务，让客户端以 stdio 运行 `python -m interlacing.cli serve --runs /绝对路径/runs --allow steps`。将 [cooling_review.json](../examples/data/cooling_review.json) 的内容作为 `start.graph`，输入 `{"steps":100}`。B1 会收到 B2 的初稿 36.6032。用同一运行编号调用 `show`（`timeline: true`），可看到 B2 已经运行、B1 正在等人。在这个示例里，初稿误把百分数当作比例。用 `answer` 提交 `{"units":"percent","reading":36.6032}`，再调 `resume`。完成后的 `outputs.report` 只含 0.366，`used.simulations` 为 1。

PostgreSQL 协作服务支持项目成员、人工任务认领和多个工作进程。[启动方法和 HTTP 接口](POSTGRES.md)

<a id="maintain"></a>

## 9 · 维护地图和排错

**发布新版本。** 更新 `src/interlacing/__init__.py` 的 `__version__`，同步两版手册中测试会检查的 pip 安装示例，把变更日志的待发布内容归入新版本。每个版本使用新标签，名称为 `v` 加完整包版本。例如 Python 版本 `0.17.0b1` 对应标签 `v0.17.0b1`。标签指向通过验证的提交，测试版在 GitHub 勾选预发行。发布后工作流自动测试、构建并上传 PyPI。另行核对同一提交的 PostgreSQL CI 作业。旧标签和旧安装包可以继续使用。[发布工作流](../.github/workflows/release.yml) · [CI 工作流](../.github/workflows/ci.yml)

改一个行为时，先找拥有这条规则的层，再找能观察到它的测试。

| 修改意图 | 先读 | 验证行为 |
|---|---|---|
| 端口、类型、接线合法性、激活契约 | [base](../src/interlacing/base/) | [基础测试](../tests/test_base.py) |
| 什么时候就绪、如何投递、怎样恢复 | [engine](../src/interlacing/engine/) 的 scheduler、wiring、recorder、journal | [引擎](../tests/test_engine.py) · [故障注入](../tests/test_faults.py) |
| 修改某种边的编码、读取或内容库 | [protocols](../src/interlacing/protocols/) | [协议测试](../tests/test_protocols.py) |
| 加一种节点、接设备、包装工具 | [nodes](../src/interlacing/nodes/) | [函数适配](../tests/test_light.py) · [远程节点](../tests/test_remote.py) |
| 调整拓扑或可复用组合 | [methods](../src/interlacing/methods/) · [blocks](../src/interlacing/blocks/) | [方法](../tests/test_methods.py) · [块](../tests/test_blocks.py) |
| 接新模型或调整模型返回值解析 | [models](../src/interlacing/models/) | [提供方](../tests/test_providers.py) · [Agent](../tests/test_agent.py) |
| 更改 JSON、允许的能力或成图指南 | [spec](../src/interlacing/spec/) 的 load、catalog、guide | [图格式](../tests/test_spec.py) · [动态子图](../tests/test_spawn.py) |
| 修改一行式 Python 入口、运行目录、命令或 MCP 工具 | [quick.py](../src/interlacing/quick.py) · [runs.py](../src/interlacing/runs.py) · [cli.py](../src/interlacing/cli.py) · [serve.py](../src/interlacing/serve.py) | [CLI](../tests/test_cli.py) · [MCP](../tests/test_serve.py) |

| 症状 | 第一个检查点 |
|---|---|
| 编译时报端口或图错误 | 查看 `GraphError` 上的节点和端口；核对输出名、输入名、协议和回边。JSON 先跑 `check`，解析错误看 `SpecError` |
| 节点没动 | 确认所需输入是否到齐、是否只有 `state` 更新、节点是否暂停，或运行是否在等人 |
| 出口空，或结果与预期不同 | 先看 `outputs` 的全部值、`rounds`、`problems`、`complete`，再定位是否被更正、轮数耗尽或输出检查失败 |
| 恢复后重复调用外部模拟 | 检查副作用发生点与日志提交点。能否让外部服务用 `ctx.key` 查回同一次任务？ |
| 新功能在 Python 生效、JSON 中无效 | 检查 spec 的加载和导出路径、Catalog 暴露的能力、CLI/MCP 入口的选项，并补一个往返或端到端测试 |

### 包根公开名称速查

以下名称均可从 `interlacing` 直接导入。具体签名可在当前安装版本中用 `help(interlacing.Port)` 等查看。需要所有参数、图格式和限制时查[中文设计参考](GRAPH.md)；下面的分组与包根导出保持一致。

| 模块 | 名称 |
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

`Schema` 描述允许的值。`Invalid` 表示发送时输出未通过检查，`NodeError` 表示节点错误，`OverBudget` 表示用量超限。`Transport` 是传输信封内容的扩展接口；`File` 实现路径协议，`FileOf` 限定端口接受的文件。`__version__` 给出安装版本。
