# interlacing

<p align="center"><img src="https://raw.githubusercontent.com/Li2O0577/interlacing/main/docs/images/interlacing.jpg" alt="interlacing" width="720"></p>

[English](README.md) · **中文**

把任何东西连成一张图，然后运行它。

[Python 使用与维护手册](docs/HANDBOOK.zh-CN.md)

- **节点**是任何接收信息并给出信息的东西：模型、函数、外部程序、设备、人，或者另一张图。
- **边**按一份写明的协议把信息从一个节点带到另一个节点：排队的值、只留最新值、状态、信号、流、有应答的调用、按路径传递的文件。
- **方法**是把节点连起来的现成方式：链、环、分发再汇合、树、星形、需要人同意的树、成员在被叫到时才出现的池。

引擎运行这张图并照看这次运行：输出要经过检查，用量按预算计数，每次激活都计时，所有事情写入日志，被打断的运行可以接着跑，观察者在事情发生的当时就被告知。这个包是一个内核，用来在它上面搭框架。它只依赖 Python 标准库。

## 安装

```sh
pip install interlacing
pip install -e .            # 或者从代码检出安装
```

需要 Python 3.11 或更高版本，支持 Linux、macOS 和 Windows。

## 第一张图

```python
from interlacing import Chain, run

def double(x: int) -> int: return x * 2
def show(x: int) -> str: return f"={x}"

print(run(Chain(double, show), {"in": 21}).last("out"))     # =42
```

普通函数就是节点：函数名是节点的 id，参数是输入口，类型标注是端口的类型，返回值从 `out` 出去。每个方法都同样接受函数、节点和别的方法。一个节点可以小到一个函数，也可以大到一个文件夹里的整个应用；图里只画需要分开管的那几块。

同一张图的完整写法，每个名字和协议都自己定：

```python
from interlacing import DEFAULT, Chain, Engine, FunctionNode, Port

double = FunctionNode("double", lambda x: x * 2, inputs=[Port("x", int)], outputs=[Port("y", int)])
show = FunctionNode("show", lambda x: f"={x}", inputs=[Port("x", int)], outputs=[Port("y", str)])

graph = Chain(double, show).build(DEFAULT)
print(Engine(graph, DEFAULT).run({"in": 21}).last("out"))     # =42
```

`@node` 在两者之间：只写和函数本身说的不一样的地方。

```python
@node(outputs=["kept", "dropped"], check=...)
def sort(x: list): ...
```

## 能做什么

**带工具的模型。** 工具是一条通向普通节点的调用边，所以一本词典、一个程序和一条机械臂的应答方式完全一样。

```python
from interlacing import AgentNode, GraphBuilder, SinkNode, Tool

agent = AgentNode("agent", model, system="...", tools=[Tool("lookup", "查一个词"), Tool("move", "移动机械臂")])
graph = (GraphBuilder().add(agent, dictionary, SinkNode("arm", arm.move, answers=True))
         .connect("agent.lookup", "dictionary.x", "call")
         .connect("agent.move", "arm.x", "call")
         .input("in", "agent.task").output("out", "agent.out").compile(DEFAULT))
```

`model` 是任何带有 `chat(system, messages, tools, max_tokens)` 的对象。`interlacing.models` 里有 Anthropic 接口、OpenAI 兼容接口，以及测试用的 `FakeProvider`。

**经过检查的输出。** 端口写明什么可以从它出去，以及出去的是别的东西时怎么办：节点失败，或者带着警告继续走，或者告诉节点哪里不对并允许它重发。

```python
Port("plan", dict, check=valid_plan, repairs=2)      # 模型会被告知计划为什么被拒，最多两次
```

**按路径传递的文件。** 节点发送的是相对路径，不是内容。文件在发出时检查一次，到达时再检查一次。

```python
protocols = with_files(DEFAULT, "work")
Port("chart", FileOf(".png", max_size=5_000_000), repairs=2)
builder.connect("draw.chart", "report.chart", "file")
```

**按节点分层的工作区。** 一次运行的文件都在一个工作区里。每个节点有自己的目录，`shared/` 是公共区，块多一层（`review/writer/`、`review/shared/`）。节点默认能读全部、能写自己的目录和所在各层的 `shared/`，描述里可以用 `"reads"`、`"writes"` 另行规定。工作区记下每个文件是哪个节点在哪次激活里写的：`interlacing files --dir RUN`。

**整个应用作为一个节点。** 一个放着 `node.json`（怎么启动、接收什么、给出什么）的文件夹就是一个节点，不管它用什么语言写、有多大。每次激活都在它的文件夹里启动它，把输入作为一个 JSON 对象交给它，再收回一个；读写纯文本的普通命令行程序也能接。图文件用 `{"kind": "folder", "path": "solver"}` 指名文件夹，而且只能在启动的人给的目录里（`--apps 目录`）。

```python
solver = FolderNode("apps/solver")      # apps/solver/node.json: {"command": ["python", "main.py"], "inputs": ["problem"], "outputs": ["answer"]}
```

**放在别处的节点。** 需要在多次激活之间一直开着的节点（打开的设备、加载好的模型）放在它所在的进程或机器上服务，图里用 `RemoteNode` 代表它。连接是 TCP 上一行一个 JSON 对象，发出的内容、用量和停止信号都能过去。不在本机监听的服务必须有令牌，令牌从环境变量读；图文件只能连启动的人允许的地址（`--remote 地址`）。

```python
serve_node(arm)                                       # 或者在另一台机器上：interlacing node-serve mypkg.arm:arm --listen 0.0.0.0:7000 --token-env ARM_TOKEN
far = RemoteNode("arm", "tcp://lab-pc:7000", token=os.environ["ARM_TOKEN"])
```

**在自己的区域里自由干活的节点。** `WorkerNode` 是一个 agent，自带操作区域内文件的工具（列目录、读、写、改、删、移动）和一个在自己目录里运行命令的工具。输入输出和普通 agent 一样，也一样经过检查。

```python
worker = WorkerNode("solver", model, Workspace("work").area("solver"), outputs=[Port("report", FileOf(".pdf"))])
```

**工具池。** 一个放工具文件的目录：Python 文件里一个函数，它的说明文字和参数就是工具的说明和参数；或者一个 JSON 文件，指向任意程序。agent 按路径拿工具（`"tools": ["plot.py", "charts"]`）。每次使用都在单独的进程里、在节点自己的目录里运行，超时被杀掉。哪个目录是工具池由启动运行的人决定（`--tools 目录`），图自己定不了。

```python
agent = AgentNode("analyst", model, tools=ToolPool("tools").tools("charts"))
```

**把设计当作单元使用。** 一张已经想清楚的图成为一个块，可以多次使用，也可以和别的块拼接。

```python
review = Block(lambda: Loop(writer(), critic(), max_rounds=3), name="review")
study = seq(clean, par(review, review, review), publish)       # 顺排、并排、嵌套
graph = Chain(intake, study("study"), report).build(DEFAULT)
```

**块门口的收发台。** `Desk` 接收从多处到来的东西，整理之后分发到多处。放在块的入口，它把工作分给块内的成员；放在块的出口，它把成员给出的结果整合成一份答复。整理需要判断力时，换成一个输入输出相同的模型节点站在同一个位置。

```python
team = Fan(analyst, modeller,                                   # 每个成员按名字拿到自己的那一份
           split=Desk("door", takes=["brief", "data"], gives=["analyst", "modeller"], sort=share),
           join=Desk("exit", takes=["analyst", "modeller"], sort=report))
```

**不事先固定的图。** 节点用一个显式操作改它所在的图：`ctx.change(Change().add(...).connect(...))`。改动按建图的全部规则检查，要么整个被拒绝，要么立刻生效；它和提出它的那次激活一起写进日志，恢复时照样重做。`Pool` 为成员留好位置，成员在被叫到之前什么也不做。`SpawnNode` 构建并运行一张由节点在运行中写出的图，范围由创建这个节点的人限定。

```python
grow = SpawnNode("grow", Catalog(allow=["my_steps"]), max_nodes=20)
graph = Chain(planner, grow).build(DEFAULT)                    # planner 发出 {"graph": ..., "inputs": ...}
```

**被照看的运行。**

```python
engine = Engine(graph, DEFAULT, workers=4,
                journal=Journal("run.jsonl"),                  # 被打断的运行用 resume() 接着跑
                time_limit=600,                                # 一次激活最长可以用多久
                budget={"tokens": 2_000_000},                  # 节点上报用量；超出则运行停止
                observe=print)                                 # 每个事件，在发生的当时
```

**每个节点都有开关。** 运行中可以对任何一个节点、或者一批节点暂停、恢复、停止、重启。操作者能拨（`engine.control`、`interlacing control`、MCP 工具 `control`），管着它的节点也能拨。谁管谁写在图里（`manage("lead", "a", "b")`），一个节点只能拨它下面的节点。正在运行的节点做完手上这一步就停（agent 是在两次调用模型之间）；暂停时就停在那里等着，位置和对话都保留。每次拨动都写进日志。开关也管到进程外面：在别的进程里跑着的运行，通过运行目录拨；`RemoteNode` 被暂停时，远端停在它自己的下一步；节点启动的外部程序会跟着被挂起（Linux 和 macOS），程序自己的时限也停表。

**常驻的 agent。** `AgentNode(..., resident=True)` 从开始到结束是一次激活，什么时候结束由 agent 自己说了算。哪部分做好了就先发哪部分；可以等别人的消息（`wait`，等的时候不占工作名额，不会再有消息时会明说）；有任务栏（`plan`、`done`、`note`），进度会存档，运行中断后接上时从原处接着做。底下是任何节点都能用的两件事：`ctx.receive()` 在激活中途收下一条消息，`ctx.commit()` 存档进度。普通函数也能直接当工具：`AgentNode(tools=[check, time_left])`，由 agent 自己调用，不用节点和边。

**更正。** `ctx.emit` 返回消息的编号（`"out#3"`），`ctx.correct(编号, 新内容)` 用新内容顶替它。对方还没读的，直接换掉；已经读了的，看对方的 `on_correction`：程序节点用更正后的值重跑，并把自己发出的也跟着更正（`rerun`，默认）；agent 被告知，自己决定（`tell`，agent 的默认）；或者不管（`ignore`）。图的输出原地改对。常驻 agent 有 `correct` 工具。

**质疑。** 边可以接受驳回：`connect(a, b, challenges=2)`。收的一方判断收到的是否符合预期，只答是或否；不符合就调用 `ctx.reject(端口, 理由)`，等上游解释（原值不变）或者更正（更正就是答复）。这条边一共能接受这么多次驳回。在这种边上收消息的 agent 会先看到一句默认提示（可以换），让它先判断，并有 `reject` 工具；消息被驳回的 agent 用 `explain` 或 `correct` 答复，已经结束的会被叫回来。程序节点默认回答“结果不变”，需要别的行为就写 `challenged`。一个端口接着几条边时，`ctx.reject(端口, 理由, which)` 用发送方点名驳回哪一条；驳回里列着同一次发出的其他消息，上游可以一起更正。

**自己组队的 agent。** 默认关闭：`AgentNode(..., resident=True, hire=3, templates={"researcher": make})` 整次运行最多招 3 个，只能从模板里招。每个下属成为它管的一个节点，有一条线派活（`send_to_<name>`）、一条线回话（`from_<name>`）；它还有 `dismiss`、`control`、`team`、`connect`（只限自己团队）。能招人的下属，名额不超过上级；目录放在上级目录下面。下属失败不会让整次运行失败：上级会被告知，自己决定重启、解雇还是放弃（`fail`）；任何上级设了 `oversees = True` 都可以用 `ctx.failures()` 这样接手。底下是 `ctx.change` 加的两个新操作 `ports` 和 `manage`；节点内部那张图里的节点用路径名拨（`"box/inner"`）。

```python
graph = GraphBuilder().add(lead, a, b).manage("lead", "a", "b")...      # lead 里：ctx.control("a", "pause")
engine.control(["a", "b"], "restart")                                  # 从外面：a 和 b 重新开始手上的工作
```

不能在图外把同一件事做两遍的节点把它包进 `ctx.once`：结果记进日志，这次激活重跑时直接拿回结果，不再执行。

```python
receipt = ctx.once("charge", lambda: bank.charge(order, key=ctx.key))   # ctx.key 是这次激活的名字，每次重试都相同
```

**协程节点，以及可以 await 的运行。** 节点的 `run`（或者交给 `FunctionNode`、`@node` 的函数）可以是 `async def`。这时引擎在事件循环上运行：这些节点在循环里被 await，不各占一个线程，所以一百个等模型回答的 agent 用 `workers=100` 就行，不会多出线程；别的节点照旧放进线程。超时的协程节点会被取消。这样的节点要调用别的节点，写 `await ctx.acall(port, payload)`。调用方是协程时，`arun` 就在调用方的事件循环上跑这张图；取消等待会停下运行。

```python
result = await Engine(graph, DEFAULT).arun({"in": 21})          # 还有 aresume() 和 ago()
```

**可以被终止的函数。** 线程无法从外部停止，所以在引擎自己进程里超时的函数只能被放弃。`IsolatedNode` 让函数在单独的进程里运行，超时或运行停止时把它杀掉；它崩溃只会让这个节点失败，不影响别的。

```python
solve = IsolatedNode("solve", "mypkg.solver.solve", inputs=[Port("problem")], outputs=[Port("answer")], timeout=300)
```

**写成数据的图。** 图文件里只有结构和与默认值不同的部分；提示词是一个被引用的文件。

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
cd examples/data && export PYTHONPATH=.
interlacing check  release.json --allow steps
interlacing run    release.json --dir /tmp/run --allow steps --input topic=tides    # 在等人回答，退出码 3
interlacing answer --dir /tmp/run --json '{"approved": true}'
interlacing resume --dir /tmp/run                                                   # 完成，退出码 0
```

一次运行就是一个目录。图里的人还没有回答时，不会有进程一直等着：命令退出，等回答给出后运行再继续。同一个目录同一时间只由一个进程运行。

**作为 MCP 服务。** 同样的操作可以提供给另一个应用里的模型。它的图能触及什么、能用多少、一个节点能跑多久，由启动服务的人在命令行上设定，客户端改不了。

```json
{"mcpServers": {"interlacing": {"command": "interlacing",
  "args": ["serve", "--runs", "/path/to/runs", "--allow", "mypkg", "--budget", "tokens=500000"]}}}
```

工具调用并行应答，互不阻塞。`start` 和 `resume` 可以带 `wait: false` 立即返回，之后用 `status` 查询，用 `stop` 提前结束。

模型密钥从环境变量读取（`ANTHROPIC_API_KEY`、`OPENAI_API_KEY` 以及对应的 `_BASE_URL`），不会写入图文件、运行目录或日志。

## 用来做什么

三个展开讲的案例在 [docs/CASES.md](docs/CASES.md)。简要如下：

| 案例 | 怎么搭 |
|---|---|
| 具身智能体：多模态模型把夹爪位姿规划成空间中的点，控制器执行，下一次观测决定接下来做什么 | `Loop(observe, planning, act)`；模型的测量工具是调用边上的节点；动作约定是计划端口上的检查，所以坏计划带着原因回到模型手里，到不了控制器；计划次数和秒数是预算。用替身可以直接运行：[examples/graphs/planner_and_arm.py](examples/graphs/planner_and_arm.py) |
| 建模助手：一个组长，每个问题一个负责人，求解者和质疑者争论到答案被证明为止，人可以在任何地方介入 | 每个问题是一个装着 `Loop(solve, critic)` 的 `Block`；“没有证明就不算完成”是一个端口检查；问题用 `seq` 依次进行；人是 `Governed` 树的操作者；运行目录在重启后仍然有效 |
| 数学家：组长并行派出探索者，每个探索者把成果交给证明检查器，通过后才汇报 | `Pool(lead, explorer, size=10)` 加 `workers`；每个探索者是一个 `Loop(prove, check)`；给出三个方向，就只存在三个探索者 |

内核负责传递信息和照看运行。模型怎么想、机械臂怎么动、论文怎么排版，是节点自己的事。

## 组成

| 层 | 包 | 主要名字 |
|---|---|---|
| 抽象 | `interlacing.base` | `Node`、`Port`、`Schema`、`Edge`、`Protocol`、`Envelope`、`Graph`、`GraphBuilder`、`Method`、`Context` |
| 引擎 | `interlacing.engine` | `Engine`、`Result`、`RunError`、`Journal`、`Transport` |
| 部件 | `interlacing.nodes`、`.protocols`、`.methods`、`.models` | `FunctionNode`、`node`、`as_node`、`AgentNode`、`ProcessNode`、`FolderNode`、`SourceNode`、`SinkNode`、`HumanNode`、`GraphNode`、`IsolatedNode`、`WorkerNode`、`RemoteNode`、`serve_node`、`Desk`；`Workspace`、`ToolPool`；`Chain`、`Loop`、`Fan`、`Tree`、`Star`、`Governed`、`Pool` |
| 块 | `interlacing.blocks` | `Block`、`seq`、`par` |
| 写成数据的图 | `interlacing.spec` | `dump`、`load`、`read`、`write`、`Catalog`、`SpawnNode` |
| 对外 | `interlacing.runs`、`interlacing.cli`、`interlacing.serve`、`interlacing.quick` | `Run`；命令 `interlacing`；MCP 服务；`run` |

每一层只使用它下面的层。引擎不认识任何具体的节点、协议或方法，`tests/test_engine.py` 里有一个测试守着这一点。引擎内部由几个各管一件事的小部件组成：图变成的信箱（`wiring`）、节点何时能运行（`scheduler`）、检查什么以及出问题后怎么办（`checks`）、预算（`ledger`）、日志（`recorder`）、时限（`clock`）。

完整说明见 [docs/GRAPH.md](docs/GRAPH.md)（英文版 [docs/GRAPH.en.md](docs/GRAPH.en.md)）。十九个可以直接运行的例子在 [examples/graphs/](examples/graphs/)，写成 JSON 的图在 [examples/data/](examples/data/)。

## 测试

```sh
python -m pip install -e . pytest
python -m pytest -q tests
python scripts/bench_engine.py       # 引擎给每个信封增加的开销（约 11 微秒）
```

测试在 Linux、macOS 和 Windows 上运行，使用按脚本应答的模型、替身模型服务和替身设备。其中包括在随机位置打断的运行，日志像进程突然死掉时那样被截断，这些运行必须和没被打断的运行得到同样的结局。还包括用 MCP 的参考客户端（`mcp` 包，装了才跑）通过服务完整跑一张图。这个包通过 `mypy` 检查。

## 限制

- 接着跑是“至少一次”：被打断的节点会重跑。`ctx.once` 让图外做过的事不再重复，只有“做完”到“记下”之间的一瞬间例外；要覆盖这一瞬间，把 `ctx.key` 交给外部服务去重。
- 边只在进程内。节点可以在别处（`RemoteNode`），但节点之间的流和调用不跨进程；`Transport` 是做这件事的位置，目前那里什么都没有。
- 开关和上下级只在同一张图里：节点内部再跑的一张图里的节点，从外面拨不到。Windows 上被暂停的外部程序会继续跑，只是节点在等。
- 两种模型接口都用替身服务测过：工具、图片、思考、流式，以及服务繁忙时的等待重试。在真实服务上只用很小的请求试过一次，思考和图片输入没有在真实服务上试过。设备节点只接触过替身。
- 在引擎自己进程里超时的节点是被放弃，而不是被杀掉。`IsolatedNode` 和 `ProcessNode` 会被杀掉。
- `allow` 是一份模块清单，图可以点名其中的函数。它不是沙箱，`IsolatedNode` 也不是：它的进程拥有启动它的进程的全部权限。worker 的命令和工具池里的工具也一样：文件工具出不了节点的区域，命令可以。
- MCP 服务只走 stdio。测试里由参考客户端跑通过，还没有接过桌面应用。

## 许可

MIT
