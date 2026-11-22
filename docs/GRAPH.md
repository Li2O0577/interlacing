# 节点、边、方法

[English](GRAPH.en.md) · **中文**

这个包把任何东西接成一张图并运行它。图里的节点可以是模型、函数、外部程序、设备、人，也可以是另一张图；边是写好的传递协议；方法是把节点接起来的现成结构。

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

## 分层

| 层 | 包 | 内容 |
|---|---|---|
| 抽象 | `interlacing.base` | `Node`、`Port`、`Schema`、`Edge`、`Protocol`、`Envelope`、`Graph`、`GraphBuilder`、`Method` |
| 引擎 | `interlacing.engine` | `Engine`、`Result`、`Journal`、`Transport`、`Stream` |
| 现成部件 | `interlacing.nodes`、`interlacing.protocols`、`interlacing.methods`、`interlacing.models` | 见下 |
| 块 | `interlacing.blocks` | `Block`、`seq`、`par` |
| 数据格式 | `interlacing.spec` | `dump`、`load`、`read`、`write`、`Catalog` |
| 对外接口 | `interlacing.runs`、`interlacing.cli`、`interlacing.serve` | `Run`；命令 `interlacing`；MCP 服务 |

引擎只认识抽象层，不认识任何具体的节点、协议或方法。`tests/test_engine.py` 里有一个测试检查各层的引用方向。

## 节点

节点声明自己的输入口和输出口，实现一个 `run(ctx, inputs)`。它不知道自己在哪张图里，上下游是谁，输出怎么传递；它碰到的外部一切都经过 `ctx`：

- `ctx.emit(port, payload)`：从一个输出口发出，一次激活里可以发多次；
- `ctx.call(port, payload)`：从一个调用口发出请求并等待回答；
- `ctx.stopping()`：运行正在停止，长时间的激活应当尽快返回。

`run` 也可以直接返回 `{输出口: 内容}`。`open`、`close`、`snapshot`、`restore` 是可选的。

端口：`Port(name, schema, required=True, many=False, call=False)`。`schema` 描述内容的形状，建图时检查两端是否相容，运行时检查每一份内容。一个不声明形状的输出口（什么都可能给）可以接到任何输入口上：建图时放行，内容到达时按接收口的形状检查，不合就按接收口的 `invalid` 失败或警告。两端都声明了而且不相容（比如 `int` 接 `str`）仍然在建图时拒绝。`many` 的输入口可以接多条边，收到的是列表。`call=True` 的输出口用来发起调用。

触发方式：`Firing.ALL`（默认）等所有必需输入到齐后运行一次；`Firing.ANY` 每到一份输入就运行一次。

### 轻写法

一个节点可以小到一个函数，也可以大到一个文件夹里的整个应用；图里只画需要分开管的几块（要并行的、要换模型的、要人介入的、要中断后接着跑的），其余都是节点内部的普通代码。为了不让“接进图”本身变成负担，同一个内核有三层写法，最后都是同一种节点、同一个引擎，随时可以从轻的换成重的：

1. **普通函数**。函数名是节点 id，参数是输入口（有默认值的是可选输入），类型标注是端口的形状（`int`、`str`、`list[float]`、`dict`；`float` 也收整数；联合类型等不按形状检查），返回值从 `out` 出去，标注 `-> None` 就没有输出；第一个参数叫 `ctx` 时它拿到的是上下文。`Chain`、`Loop`、`Fan`、`Tree` 等方法直接收函数，也直接收别的方法（成为一个以方法名命名的 `GraphNode`）；同一个函数出现两次，第二个叫 `名字_2`。`as_node(f)` 把函数变成节点，lambda 没有名字，要给 id：`as_node(f, "名字")`。
2. **`@node`**。写在函数定义上，只写和默认不同的地方：`@node(outputs=["kept", "dropped"], check=..., repairs=2, firing=Firing.ANY, id=...)`。被装饰的仍然可以当函数直接调用。
3. **完整写法**。`Node` 子类、`Port`、`GraphBuilder`，全部照旧。

协议也可以不写：`build()`、`compile()`、`Engine(graph)`、`GraphNode(id, graph)`、`ctx.engine(graph)` 不给协议时用 `DEFAULT`，图记住自己建时用的协议，引擎默认用它；`connect(a, b)` 不写协议时是 `value`。`interlacing.run(东西, 输入, **引擎参数)` 一步建图并运行，东西可以是图、方法、节点或函数；单个节点或函数时，图的输入输出就是它的端口名。

```python
from interlacing import Chain, Fan, node, run

def clean(text: str) -> str: return " ".join(text.split())
def words(text: str) -> int: return len(text.split())
def letters(text: str) -> int: return sum(c.isalpha() for c in text)

@node(check=lambda line: not line.startswith("0 ") or "没有词")
def report(x: list) -> str: return f"{x[0]} 词, {x[1]} 字母"

run(Chain(clean, Fan(words, letters), report), {"in": " the cat "}).last("out")
```

### 现成的节点

| 节点 | 是什么 |
|---|---|
| `FunctionNode` | 一个 Python 函数（`FunctionNode.of(f)`、`as_node(f)`、`@node` 从函数本身读出端口） |
| `FolderNode` | 一个文件夹里的整个应用，见下面“整个文件夹作为一个节点”。写在 JSON 图里是 `"kind": "folder"` |
| `AgentNode` | 一个模型。工具是调用口，也可以直接是普通函数（见下）；有多个输出口时由模型用 `send_<口名>` 选择。模型是任何带 `chat` 方法的对象，`interlacing.models` 里有现成的。`memory` 可以是一个函数，见下。`resident=True` 时是常驻 agent，见“常驻 agent” |
| `ProcessNode` | 一个外部程序，输入写到标准输入，标准输出作为输出 |
| `IsolatedNode` | 一个 Python 函数，在单独的进程里运行：超过运行的时间上限、自己的 `timeout`，或者运行被停止时，进程被杀掉；它崩溃只让这个节点失败。函数必须是可导入模块顶层的函数（或它的带点名字），输入和返回值以 JSON 在两个进程之间传递，拿不到 `ctx`。它不是沙箱。写在 JSON 图里是 `"kind": "isolated"`，需要目录允许启动进程 |
| `WorkerNode` | 在给定工作区里自由操作的 agent：自带文件工具和运行命令的工具，输入输出照旧。见下面“工作区、worker 和工具池”。写在 JSON 图里是 `"kind": "worker"` |
| `SourceNode`、`SinkNode` | 外部读数的来源、结果的去处（相机、机械臂这类设备接在这里） |
| `HumanNode` | 人。请求交给 `ask(request)`，它返回时就是人的回答 |
| `GraphNode` | 一整张图当成一个节点，方法由此嵌套；块的每一次使用就是它 |
| `SpawnNode` | 运行中收到一张写成数据的图，建出来、运行、发回结果（在 `interlacing.spec` 里，见“运行中增长”） |
| `Pass`、`Collect`、`Branch`、`Until` | 只负责流向：原样转发、汇合成列表、二选一、循环是否继续 |
| `Desk` | 收发台：`takes` 是它接收的各个来处，`gives` 是它分发的各个去处，`sort(received)` 把收到的整理成“去处到内容”的映射（没有 `sort` 时每个去处都拿到全部）；`wait=False` 时来一份处理一份。放在块的入口是接收员，放在出口是发送员；需要判断力时换成输入输出相同的 `AgentNode` |
| `Roster` | 管着固定数目的位置，按名字把位置发出去；`Pool` 用它 |

`AgentNode` 留了两个口子，默认都不起作用：

- `memory=函数`：每次请求模型之前，`memory(messages)` 的返回值成为新的对话。压缩、摘要、只留最近几轮，都写在这个函数里，内核不规定做法。
- 图片：到达的内容里只要有 `{"mime": "image/png", "data": <base64>}`，就作为图片交给模型，文字里留一个 `[image 1]` 的位置。

- 普通函数当工具：`AgentNode(..., tools=[check, time_left])`。函数名就是工具名，文档字符串是说明，参数和类型标注生成参数的 JSON schema，没有默认值的参数是必填的。名为 `ctx` 的参数不由模型填，而是交给这个节点的上下文。这样的工具由 agent 自己调用，图里不需要节点和边。`Tool.of(函数, 名字=None, 说明=None)` 是显式写法；图文件里写 `{"function": "模块.名字"}`，受 `allow` 约束。自带的 `time_left` 告诉 agent 离时间上限还有多少秒。定时提醒、验证程序这类东西写成几行函数交给 agent 就行，不必每个程序都做成图里的节点。

系统提示词默认是空的，包不替你写提示词。要限定一个智能体读写哪些文件、遵守什么目录结构，写在它的 `system` 里；需要硬性限制时，把读写文件做成一个工具节点，由那个节点检查路径。

### 整个文件夹作为一个节点

一个文件夹里放一个 `node.json`，写明怎么启动这个应用、它接收什么、给出什么，这个文件夹就是一个节点。应用可以用任何语言写、有任何大小，不需要知道 interlacing：

```json
{
  "command": ["python", "main.py"],
  "inputs": ["problem", {"name": "data", "required": false, "type": "list"}],
  "outputs": ["answer", "notes"],
  "timeout": 600
}
```

- 每次激活在这个文件夹里启动 `command`。以 `python` 开头的命令用运行图的那个 Python。
- 到达的输入作为一个 JSON 对象（输入名到值）写到标准输入，也写在环境变量 `INTERLACING_IN` 指的文件里。
- 应用的回答是一个 JSON 对象（输出名到值）：写进 `INTERLACING_OUT` 指的文件，这样标准输出可以随便打印；没写这个文件时，整个标准输出就是回答。没出现的输出口不发东西，出现了不认识的输出名算失败。
- `"format": "text"`：最多一个输入、一个输出，读写纯文本，适合普通命令行程序。
- 端口的 `type` 可以是 `any`（默认）、`int`、`float`、`number`、`str`、`bool`、`list`、`dict`。
- 非零退出算失败，报错带上标准错误的最后一段；超过 `timeout`、运行的时间上限或运行被停止时，进程被杀掉。图有工作区时，`INTERLACING_WORKSPACE` 是它的路径。
- 它不是沙箱：应用有启动它的进程的权限。

在 Python 里：`FolderNode("apps/solver")`，id 默认是文件夹名。写在 JSON 图里是 `{"id": "solve", "kind": "folder", "path": "solver"}`：路径相对于启动的人给的应用目录（`Catalog(apps=目录)`，命令行和 MCP 服务是 `--apps 目录`），出了这个目录就拒绝，而且和 `process` 一样需要允许启动进程。`node.json` 的内容进了图的摘要，改了它，旧的运行要用 `resume(change=True)` 接着跑。

### 在别的进程或别的机器上的节点

`FolderNode` 每次激活都重新启动程序；有些东西应该一直开着，跨越多次激活保留它拿着的东西：一台打开的设备、一个加载好的模型、一个连接。这样的节点放在它所在的进程里“服务”，图里用 `RemoteNode` 代表它：

```python
server = serve_node(arm)                     # 远端：任何节点或普通函数；也可以在命令行里 interlacing node-serve
far = RemoteNode("arm", server.address)      # 图里：端口向远端问来，每次激活在远端执行
run(Chain(plan, far), {"in": task})
```

- 在另一台机器上：`interlacing node-serve mypkg.arm:make --listen 0.0.0.0:7000 --token-env ARM_TOKEN`。目标写成 `模块:名字`，是节点、普通函数，或者加 `--make` 时是造出节点的函数。它先打印一行 `{"address": ..., "node": ...}`，然后一直服务到被中断。
- 连接上是一行一个 JSON 对象（UTF-8）：`hello`（带令牌）之后，每次激活发一条 `run`（带输入、轮次和 `ctx.key`），远端回 `emit`、`use`，最后 `done` 或 `error`；运行要停下时近端发 `stop`，远端的 `ctx.stopping()` 随之变真。
- 过去和回来的都只能是普通数据。远端一次只跑一次激活，不论是谁请求的；`ctx.call` 被拒绝（远端够不到图里），`ctx.once` 每次都会做，因为远端没有日志。远端用量通过 `use` 回到这次运行，受预算约束。
- 连接在激活中途断了，这次激活失败，可以照常重试或从日志恢复；下一次激活会重新连接。
- 不在本机（`127.0.0.1`）监听的服务必须有令牌，每个客户端都要带上。这不是加密：跨机器时放在已经可信的网络里，或者经过隧道。
- 写在 JSON 图里是 `{"id": "arm", "kind": "remote", "address": "tcp://host:7000", "token_env": "ARM_TOKEN"}`。图文件只能连启动的人允许的地址（`Catalog(remotes=[...])`，命令行和 MCP 服务是 `--remote 地址`，可以重复）；令牌只从 `token_env` 指的环境变量读，不写进图文件、运行目录或日志。端口不写时在建图时向远端询问，所以建图时远端要已经在服务。

## 边和协议

边从一个输出口到一个输入口，带一个协议名。协议决定什么能通过、怎么投递、到达时是否启动接收节点。

| 协议 | 投递 |
|---|---|
| `value` | 每份内容一个信封，按顺序排队，一次激活取一个 |
| `latest` | 只保留最新的一个，旧的未读即被替换 |
| `state` | 最新值随时可读，但到达不会启动节点，读取也不消耗 |
| `signal` | 没有内容，只有到达这件事 |
| `stream` | 发送方一次激活发出的内容是一个序列，接收方边写边读 |
| `call` | 请求和回答：发送方等待，接收节点为这次请求运行，返回值是回答 |
| `file` | 边上传的是文件的相对路径，文件本身不动（`with_files(DEFAULT, 工作目录)`，见“按路径传文件”） |
| `artifact` | 内容存进内容库，信封里只有引用（哈希、大小、类型）（`standard(store)`） |

`DEFAULT` 是前六种的注册表；`standard(store)` 另外带上 `artifact`。自己的协议继承 `Protocol`，实现 `check`、`encode`、`decode`，注册进 `Protocols`。

回边：`connect(..., back=True, max_rounds=N)`。去掉回边后图必须无环，每条回边必须真的闭合一个环，并且带轮数上限，所以任何图都不会无限运行。

## 输出检查

节点发出的每一份内容，在发出的那一刻被检查三次：端口的 `schema`（形状对不对），每条边的协议（比如 `file` 协议看文件在不在），最后是端口自己挂的检查函数：

```python
def positive(value):
    return None if value > 0 else f"{value} 不是正数"      # 返回字符串说明哪里不对；返回 False 或抛出异常也算没通过

Port("y", int, check=positive)                 # 可以是一个函数，也可以是几个
```

检查没通过时怎么办，由端口决定：

| 写法 | 效果 |
|---|---|
| `invalid="fail"`（默认） | 节点失败，运行停下。错误信息指出节点、轮次、端口和原因。问题处理好后可以 `resume()` |
| `invalid="warn"` | 内容照原样发出，问题记录在案，运行继续 |
| `repairs=N` | 在上面两种处理之前，先把问题告诉这个节点，让它改了重发，一次激活里最多 N 次 |

`repairs` 的纠正有两种走法。节点自己会改的（智能体就是这样），`ctx.emit` 抛出 `Invalid`，它读 `problem`，在 `again` 为真时改正后重发。节点不处理这个异常的，引擎用同样的输入把它重跑一次，`ctx.problems` 里是上次的问题；但如果它这次激活已经发出过别的内容，就不重跑而是直接失败，以免同一份内容发两遍。

`repairs` 不写时由节点决定：普通节点是 0，`AgentNode` 是 2。智能体被纠正时，问题的原话会作为一条消息交给模型，它手里的工具都还在，所以可以去把缺的文件写出来。要求 JSON 而模型答的不是 JSON，走的也是这条路。

每个问题不论结局如何都留下记录，去处有四个：`Result.problems`、日志、监听口的事件（`rejected` 是打回给节点，`warned` 是放行），以及运行失败时异常上的 `problems`。每条记录是 `{"node", "round", "port", "problem", "handled"}`。

输入口上只有 `invalid` 起作用：到达的内容和发出时不一样（比如文件在路上被改了），接收端口是 `fail` 就让接收节点失败，是 `warn` 就照样交给节点并记录。

### 按路径传文件

`file` 协议让节点之间传文件的相对路径，而不是文件内容。所有节点在同一个工作目录里干活；发送方把文件写在工作目录下，发出路径（一个字符串，或者带 `path` 的映射，映射里其他内容会一起带过去）。信封里是路径加上发送那一刻的大小、哈希和类型，是很小的普通数据，照常进日志。

```python
from interlacing.protocols import DEFAULT, FileOf, with_files

protocols = with_files(DEFAULT, "work")                       # 工作目录
Port("chart", FileOf(".png", ".svg", max_size=5_000_000))     # 这个口发的是图片，最大 5MB
builder.connect("draw.chart", "report.chart", "file")
```

接收方拿到 `{"path": "plots/a.png", "size": 1234, "sha256": "...", "media": "image/png"}`，`protocols.get("file").locate(收到的内容)` 是文件的完整路径。

发送时按顺序检查：内容是不是一个路径；路径是相对的，而且跟随链接之后仍在工作目录内；它是一个文件，不是目录，也不是不存在；文件不是空的（`File(root, empty=True)` 允许空文件）；后缀、类型、大小符合端口上 `FileOf` 的声明。到达时再看一次：文件还在，大小和哈希没变。

错误信息是写给能改正的一方看的，比如 `there is no file chart.svg in the working directory (the working directory holds: data)`。

出了问题怎么办，完全按上面的端口规则走：默认节点失败、运行停下可续跑；`invalid="warn"` 时路径照样送达，但带着 `"unchecked": true`，接收方知道它没被核实；`repairs` 让节点有机会把文件补上。内容连路径都不是的时候，警告也救不了，节点失败。

`AgentNode(files=工作目录)` 会把以文件形式到达的图片读出来给模型看。

### 工作区、worker 和工具池

`Workspace(目录)` 是一次运行的全部文件，路径都相对于它的顶层写，和 `file` 协议一样。布局按节点分：

```
solver/            节点 solver 的目录
shared/            所有节点都能写
review/            块 review：多一层
    writer/        块里的 writer
    shared/        这个块的成员共用
```

`workspace.area(节点, layer=[块...], reads=None, writes=None)` 是一个节点的那一份（`Area`）：默认能读全部，能写自己的目录和所在各层的 `shared/`；给了 `reads`、`writes`（相对顶层的路径列表）就按给的来，能写的地方一定能读。从图文件加载时，块里的节点自动多一层，`"reads"`、`"writes"` 写在节点上。通过区域写下的文件都记在 `.interlacing/written.jsonl`（哪个节点、第几次激活），`workspace.files()`、`Run.files()` 和 `interlacing files --dir RUN` 列出每个文件和它的来源。

普通 agent 有了区域（图文件里自动给），它从装文件的端口发出的路径必须是它能读的，否则这条路径带着原因退回给模型改。

`WorkerNode(id, model, 区域或目录, commands=True, command_timeout=120, ...)` 是一个 agent，自带 `list_files`、`read_file`、`write_file`、`edit_file`、`delete_file`、`move_file`，`commands=True` 时还有 `run_command`（一行 shell 命令，在自己目录里运行，超时连同它启动的进程一起杀掉）。它被告知自己在哪里、能读写哪里。其余都和 agent 一样：端口、检查、重做、步数、预算、时间上限，也能再接图里的工具和工具池的工具。命令改动的文件也记在它名下。图文件里是 `"kind": "worker"`；带命令时需要目录允许启动进程，`"commands": false` 则只有文件工具。文件工具出不了区域，命令可以：这不是沙箱。

`ToolPool(目录, timeout=120)` 是工具池。一个工具是一个文件：

- `plot.py`：和文件同名的函数（或叫 `run` 的函数，或唯一一个不以 `_` 开头的函数）。说明文字是给模型看的说明，参数和标注、默认值成为工具的参数。取工具时只读这个文件，不运行它。
- `fetch.json`：`{"description": ..., "parameters": {JSON schema}, "command": ["./fetch.sh"]}`，任意程序，参数以 JSON 从标准输入给它，它写出的内容就是答复（能按 JSON 读就按 JSON 读）。相对路径的程序在池里找。

`pool.tool("plot.py")` 是一个工具，`pool.tools("charts")` 是一个目录里的全部工具（不以 `_`、`.` 开头的 `.py`、`.json`）。这样的工具是节点自己的工具，不占端口也不需要边。每次使用在单独的进程里、在节点自己的目录里运行，环境变量 `INTERLACING_WORKSPACE` 是工作区顶层；超时被杀掉，失败时把原因告诉模型。图文件里 agent 和 worker 的 `"tools"` 可以写路径（`["plot.py", "charts"]`），工具池由 `Catalog(tools=目录)`、命令行和 MCP 服务的 `--tools 目录` 给出，图文件自己指定不了。

## 图

`GraphBuilder` 收集节点和边，`compile(protocols)` 检查整体并返回只读的 `Graph`。图的 `digest` 是其定义的 SHA-256，定义相同则摘要相同。改图是从原图得到一张新图：

```python
new = GraphBuilder.of(graph).remove("review").add(check).connect("draft.y", "check.x", "value").compile(DEFAULT)
```

### 每个节点的开关，以及谁管谁

运行中的每个节点都有一个开关：开着、暂停、停止。可以单个拨，也可以成批拨。动作有四个：

| 动作 | 没在运行的节点 | 正在运行的节点 |
|---|---|---|
| `pause` 暂停 | 不再开始 | 做完手上这一步，停在那里等着，位置和对话都保留 |
| `resume` 恢复 | 照常开始，处理等着它的消息 | 从停下的地方接着做 |
| `stop` 停止 | 不再开始，等着它的消息留着 | 做完手上这一步就结束这次工作；它取走的输入放回去，留给以后 |
| `restart` 重启 | 忘掉它记着的东西（`Node.reset`，agent 就是清空对话），重新开始 | 同上，并且用原来的输入重新做 |

“一步”指节点调用 `ctx.checkpoint()` 的地方。`AgentNode` 在每次调用模型和每次使用工具之前都会调用它；自己写的长任务节点每做完一项也应当调用一次，返回 `False` 就该结束（协程节点用 `await ctx.acheckpoint()`）。从来不调用它的节点，要等这次激活做完，开关才起作用。这时它做出的结果照常算数；如果拨的是 `restart`，结果算数之后它记着的东西照样清掉。正在回答别人调用（`call`）的节点被暂停时会停在下一步等着；被停止时会把这次回答做完，因为调用方还在等，之后才不再开始。

把工作交给外面的节点也跟着开关走：`RemoteNode` 被暂停时，远端节点停在它自己的下一个 `checkpoint` 等着，停止时远端也被告知；外部程序（`ProcessNode`、`FolderNode`、`IsolatedNode`、agent 调用的命令和工具程序）被暂停时，在 Linux 和 macOS 上程序本身也被挂起（Windows 上程序继续跑，只是节点等着），暂停期间程序自己的 `timeout` 不走表。自己写这类节点时可以用 `ctx.paused()` 知道现在是否被暂停。

谁能拨开关：

- 操作者：`engine.control(节点, 动作, team=False)`；运行目录用 `Run.control(...)` 或命令行 `interlacing control 动作 节点... --dir RUN`；MCP 服务用工具 `control`。运行正在进行时立刻生效；运行在别的进程里进行时（比如另开一个终端跑的 `interlacing run`），变化先放进运行目录的 `controls/`，那个进程片刻之内就会接手；没在进行时写进日志，下次 `resume` 时生效。返回的 `changed` 只列真正变了的节点。只用 `Engine`、不给它日志时，因全部暂停而先结束的运行没法接着跑：要接着跑就用运行目录，或者给 `Engine` 传 `journal`。
- 节点自己的上级：图里用 `GraphBuilder.manage("lead", "a", "b")` 写明谁管谁（图文件里是 `"manages": {"lead": ["a", "b"]}`）。这是一棵树：一个节点最多一个上级，不能绕一圈管到自己。节点在运行中用 `ctx.control(节点, 动作, team=False)` 只能拨它下面的节点（多层也算），拨别的会被拒绝。`team=True` 时连同对方管着的节点一起拨。`ctx.team()` 列出它下面的节点和各自的状态（开关、是否在干活或在等、上级、跑了几轮）。运行中加进来的节点也可以指定上级：`Change().manage("lead", "新节点")`。
- 节点内部跑的一张图（`GraphNode`、块）里的节点，用路径名拨：`engine.control("box/slow", "stop")`，再往里就是 `box/inner/x`。能拨 `box` 的人（操作者，或者管着 `box` 的节点）就能拨它里面的节点。只有 `box` 正在跑它那张图的时候才拨得到。

- **下属失败交给上级：** 一个节点的 `oversees` 为真（图文件里写 `"oversees": true`；能招人的 agent 默认如此）时，它管着的节点失败，整次运行不会因此失败。失败的节点被停下，没用完的输入留着；失败记进问题列表（`handled` 是 `reported`），交给上级。上级用 `ctx.failures()` 取走（每条有 `node`、`round`、`error`），在 `ctx.receive` 里等着时会被叫醒、拿到一个空映射。之后由上级决定：`restart` 重来、拿出图，或者自己失败，让整次运行失败。只有上级正在运行时才交给它；上级没在运行、不 `oversees`，或者拿到之后一直没取走就结束了，运行照旧失败。

开关的每次变化都写进日志，恢复后保持原样，观察函数会收到 `control` 事件。暂停的时间不计入激活的时间上限。正被暂停的节点不能被调用（`call`）。

剩下的工作全都被暂停时，运行不会一直挂着：停在暂停处的节点把输入放回去，运行以“没做完”结束，`Result.held` 列出开关没开的节点，运行目录的状态里是 `held` 和 `switches`。在等这些节点的节点（比如等下属回话的主控）也一起结束，这次运行里不会再被叫起来，所以不会反复调模型，也不会因为等不到而草草交差。之后把开关拨回来再 `resume` 就能接着跑；这和图里的人还没回答时运行先停下是同一个思路。

### 在激活中途收消息

节点不必在一次激活开始时就拿到全部输入。正在运行的节点可以用 `ctx.receive(ports=None, timeout=None)` 停下来等它的下一条消息（协程节点用 `await ctx.areceive(...)`），拿到的是 `{端口: 值}`。顺序是消息到达的顺序；给了 `ports` 时只看这几个输入端口。

```python
def asker(ctx, x):
    ctx.emit("ask", x)                       # 先把问题发出去
    answer = ctx.receive("answer")           # 等回答，等的时候不占工作名额
    return {"out": answer["answer"] if answer else None}
```

- **等待不占名额。** `workers=N` 限的是正在干活的激活，正在等的不算：线程驱动下会借出一个工作线程，等完后多出来的线程自己退出。所以 `workers=0` 或 `1` 时，一个节点等另一个节点的回答也不会卡住。
- **返回 `None` 的情况：** `timeout` 秒到了；节点被停止或重启，或者运行要结束（这时 `ctx.stopping()` 为真）；再也不可能有消息来，也就是别的激活都已经结束或者也在等。两个节点互相等时，两边都会拿到 `None`，可以各自收尾，运行不会挂住。如果这时还有被拨停的节点手里有活，运行会像全部暂停时一样先结束，之后可以接着跑。
- **等待中的开关：** 被暂停的节点不会取走新消息，直到被恢复。
- **时间上限：** 等待的时间不计入激活的时间上限。
- **记账：** 中途取到的消息算在这次激活名下，和一开始拿到的输入一起写进日志。激活被打断时，这些消息会放回信箱。观察函数会收到 `received` 事件。

节点运行时到达的消息，在它结束后照常触发下一次激活，不会丢。所以一个节点可以选择在一次激活里一直收，也可以照旧每条消息激活一次。

### 长激活的进度存档

一次激活可能很长，比如一个常驻 agent 从头到尾只有一次激活。`ctx.commit()` 把它到目前为止做的事写进日志，包括取走的消息、发出的消息和节点自己的状态（`snapshot`），然后激活继续往下做。

- **进程断了以后：** `resume` 不会从头重跑这次激活。节点先恢复成存档时的状态，再以 `ctx.resumed` 为真、输入为空的方式运行一次，从那里接着做；这期间新到的消息用 `receive` 去收。存档前发出的消息不会再发一遍。
- **被停止后再恢复：** 和进程断了一样，接着做。
- **被重启：** 不接着做，而是用一开始拿到的输入从头来。
- 存档之前取走的消息已经用掉了，停止时不再放回。
- 没有日志时 `commit` 返回 `False`，其他照旧。
- 要用 `commit` 的节点，它的 `run` 要能处理 `ctx.resumed`。

```python
def run(self, ctx, inputs):
    if not ctx.resumed:
        self.todo = make_list(inputs)
    while self.todo and ctx.checkpoint():
        ctx.emit("out", work_on(self.todo.pop(0)))
        ctx.commit()                         # self.todo 在 snapshot 里
```

### 常驻 agent

`AgentNode(..., resident=True)`（图文件里写 `"resident": true`）的 agent 不是“来一条消息、答一轮就结束”，而是一直在，直到它自己说做完了。整个过程是一次激活。它比普通 agent 多这些工具：

| 工具 | 作用 |
|---|---|
| `send_<输出>` | 只有一个输出时也有。哪部分做好了就先发哪部分，不用等全部做完 |
| `wait(seconds?)` | 等别人发来的消息（`ctx.receive`），等的时候不占工作名额。返回收到的内容和任务栏；如果不会再有消息来了（别人都做完了或者也在等），会明说 |
| `plan(items)` / `done(item)` | 往任务栏里加事项、勾掉事项（按编号或原文） |
| `note(text)` | 记下自己做到哪里了，同时存档（`ctx.commit`） |
| `correct(id, content)` | 更正自己发过的一条消息。`id` 是 `send_<输出>` 回给它的编号（如 `say#2`）；做法见下面的“更正” |
| `explain(id, text)` | 自己发的东西被驳回、但它是对的：说明理由。见下面的“质疑” |
| `hire` / `dismiss` / `control` / `team` / `connect` / `fail` | 只有打开招人（`hire=N`）时才有，见下面的“招人” |
| `finish(summary?)` | 宣布做完。还有输出一次都没发过时，第一次调用会先提醒，再调用一次才结束 |

- **任务栏：** 开始时每个输出各占一行（“还没发”/“发了几次”，更正过的还写着更正了几次），下面是 agent 自己用 `plan` 加的事项，最后是它的最后一条笔记。每次 `wait` 的结果里都会附上，它也是节点状态的一部分，会存档。
- **存档：** 每次 `wait` 之前和每次 `note` 都会存档。进程断了以后 `resume`，agent 带着原来的对话和任务栏接着做，模型会先看到一句“运行被中断后又接上了”和它的任务栏。
- **不调工具时：** 模型只回了文字、没调任何工具，会被提醒去等、去发或者结束，不会就此停下。`max_steps` 限的是两次收到消息之间调用模型的次数。
- **开关：** 和别的节点一样可以暂停、停止、重启；上级可以用 `ctx.control` 拨它。

[examples/graphs/resident_team.py](../examples/graphs/resident_team.py) 是一个主控带两个常驻组员：主控分活、等两份报告、用一个普通函数工具核对、交结果；组员各自做完先发报告，再等，得知不会再有消息后结束。

### 更正

节点发出的消息错了，可以更正。`ctx.emit` 返回这条消息的编号（`"输出端口#第几条"`，如 `"out#3"`），之后用 `ctx.correct(编号, 新内容)` 让新内容顶替它：

```python
def survey(ctx, site):
    quick = ctx.emit("area", rough(site))     # 先发个估计，让别人可以先干起来
    ctx.receive()                             # 等读过估计的人用完它
    ctx.correct(quick, careful(site))         # 再用仔细量的结果顶替
```

这条消息去了哪里，更正就跟到哪里：

- **还没被读的：** 在信箱里直接换掉，收的人只看到更正后的内容。
- **已经被读的：** 看收的节点的 `on_correction`：
  - `rerun`（默认，适合程序节点）：用更正后的值把读到它的那次激活再跑一遍，其他输入不变。重跑时发出的第 k 条消息，自动当作对第一次第 k 条的更正，所以更正会一路往下游传；多发的照常是新消息，少发的那几条保持原样。
  - `tell`（`AgentNode` 的默认）：更正作为一条新消息送到，`ctx.replaces(端口)` 给出它更正的是哪条；agent 看到的这条消息会标明“这是对之前收到的那条的更正”，由它自己决定怎么办。在激活中途用 `ctx.receive` 读到的消息，被更正时一律按 `tell` 送。
  - `ignore`：不管。
- **图的输出：** 结果里那一项原地换成新内容，不会多出一项。

规则：

- 只能更正自己发的消息；同一条可以更正多次，以最后一次为准。每个节点记着最近 1024 条可以更正的消息，更早的更正会报错。
- 更正和普通消息一样要过检查，也一样算在边的 `max_rounds` 里：边已经传满时，更正不再发出。
- 更正写进日志。运行接上以后，还没读的更正照样在信箱里；已经读过的消息再被更正，按 `tell` 送（重跑一次激活要的输入不进日志）。
- 图文件里写 `"on_correction": "tell"`；只有和这类节点的默认不同时才写进描述。
- 常驻 agent 用 `correct(id, content)` 工具更正；`send_<输出>` 会回它 `sent, as say#2` 这样的编号。
- 流和调用边上的消息不能更正。远端节点（`RemoteNode`）也能更正自己发的消息：远端的 `ctx.emit` 返回远端自己的编号，`ctx.correct` 用它，本地把它对到运行里的编号上。

[examples/graphs/corrected_estimate.py](../examples/graphs/corrected_estimate.py)：先发一个粗估，读过的人用完后再更正；算价格的程序自动重算，报价原地改对；写说明的 agent 被告知，重写了一份。

### 质疑：下游驳回，上游答复

下游不一定照单全收。一条边打开质疑以后，收的一方可以说“这不是我预期的”，驳回它，上游必须解释或者更正：

```python
builder.connect("planner.plan", "reviewer.plan", challenges=2)    # 这条边最多接受 2 次驳回
```

- **开关在边上：** 不写 `challenges` 就是关，和以前一样。写了就是打开，数字是这条边一共能接受几次驳回。流、调用和不触发的边不能打开。
- **下游只回是或否：** 不需要给出数字。符合预期就照常用；不符合就调用 `ctx.reject(端口, 理由)`，驳回这次激活在这个端口上最后取到的消息，然后停下来等（不占工作名额）。返回值是三种之一：
  - `{"answer": "explained", "text": ...}`：上游解释了，原值不变。
  - `{"answer": "corrected", "value": ...}`：上游更正了，这是新值。这条更正就是给它的答复，不再按 `on_correction` 另外处理；其他读过这条消息的节点照常按各自的 `on_correction` 处理。
  - `{"answer": "refused", "why": ...}`：这条边的次数用完了（同时记进问题列表）、上游已停止或不在了（质疑发出后上游才被停下或拿掉，哪怕正在回答，也立刻得到这个结果）、运行要结束了，或者再也不会有人回答。这时下游只能用手上的值继续。因为更正而重跑的那次激活，也可以质疑它读到的新值。
  - `ctx.challengeable(端口)` 说现在还能不能驳回。
- **有方向地驳回：** 一个输入口接着好几条边时（`many=True`），`ctx.reject(端口, 理由, which)` 用 `which` 指明驳回哪一条：发它的节点 id、边的 id 或信封编号都行，不写就是最后取到的那一条。`ctx.taken(端口)` 列出这次激活在这个端口上取到的每一条（`id`、`from`、`edge`）。每条边的次数分开算。
- **上游怎么答复：**
  - 正在运行并且在 `ctx.receive` 里等着：被叫醒，`receive` 返回一个空映射，再用 `ctx.rejections()` 取到驳回（`id`、`port`、`message`、`content`、`reason`、`by`、`sent`）。答复用 `ctx.explain(驳回 id, 说明)` 或 `ctx.correct(message, 新值)`。
  - 已经结束、或者这次激活没有答复：引擎为它单独起一次激活，调用 `Node.challenged(ctx, 驳回)`。它可以更正，也可以返回一段说明。程序节点默认回答“这是程序根据输入算出来的，不变”；需要别的行为就自己写 `challenged`。
- **同一次激活发出的其他消息：** 驳回里的 `sent` 是“输出口 → 编号列表”，列出被驳回的那条消息所在的那次激活还发了什么（最多 32 条）。比如 a 有 1、2 两个出口，2 被驳回，a 能看到 1 上发的是 `one#1`，判断它是不是因为同样的原因也错了；错了就一起 `ctx.correct`。1 的下游照常按各自的 `on_correction` 重跑或被告知，再往下也跟着更正。
- **agent：**
  - 作为下游：收到可以驳回的消息时，会附一句默认提示，让它先判断是否符合预期、不符合就驳回，同时多一个 `reject(port, reason, which)` 工具；端口接着好几条边时，提示里会写明每条是谁发的，`which` 填发送方。提示可以换成自己的话：`AgentNode(..., doubt="...")`，用 `{ports}` 代表端口名。
  - 作为上游：被驳回时，模型看到一条 `[rejected]` 消息，写着谁驳回的、理由、对方收到的内容。它用 `explain` 或 `correct` 答复。常驻 agent 在 `wait` 里收到，有 `explain(id, text)` 和 `correct(id, content)` 两个工具；普通 agent 会被叫回来，带着原来的对话答复。`[rejected]` 消息里也列出同一次发出的其他消息的编号，提示它看看要不要一起改；`correct` 带上 `id` 就能更正其中任何一条。
- **日志：** 每次驳回记在下游那次激活名下，运行接上以后，边上用掉的次数不会重新算。

[examples/graphs/rejected_answer.py](../examples/graphs/rejected_answer.py)：审阅的 agent 驳回了少一步的计划；做计划的 agent 已经结束，被叫回来更正；审阅拿到更正后的计划，通过。

[examples/graphs/directed_rejection.py](../examples/graphs/directed_rejection.py)：两个估计汇到一个端口，合并的程序点名驳回快的那个（`which="quick"`）；快的那个看到驳回里还列着它同时发出的说明，把估计和说明一起更正；已经读过说明的日志节点按新说明重跑。

### 招人：agent 组建自己的团队

常驻 agent 可以在运行中招下属，一层管一层。默认关闭，要打开并给上限：

```python
lead = AgentNode("lead", model, resident=True, hire=3, templates={"researcher": make_researcher})
```

- **开关和上限：** `hire=3` 表示整次运行里最多招 3 个；不写或写 0 就是关，连工具都没有。解雇不会空出名额。只有常驻 agent 能招，因为它要留下来听下属回话。
- **只从模板招：** `templates` 是名字到模板的映射。模板可以是一个“给 id 造节点”的函数，也可以是一段节点描述。图文件里写 `"hire": 3, "templates": ["researcher"]`，名字指向 `Catalog(templates={...})` 里登记的描述；也可以直接写描述 `{"researcher": {...}}`。下属如果也能招，它的 `hire` 不会超过上级的。
- **工具：**

| 工具 | 作用 |
|---|---|
| `hire(template, name, task?)` | 按模板招一个下属，id 是 `<自己>-<name>`。它归自己管；自己多出一个输出 `to_<name>`（用 `send_to_<name>` 派活）和一个输入 `from_<name>`（下属的第一个输出接回来，是一条回边，最多 `hire_rounds` 轮，默认 10）。给了 `task` 就马上派出去 |
| `dismiss(name)` | 停掉这个下属和它下面的所有人，再从图里拿掉；还在干活的，等它停下后再拿掉（下次调模型前），这之前名字不能再用 |
| `control(name, action)` | 暂停、恢复、停止、重启下属（连同它下面的人）；`name` 可以带路径，拨到下属内部的图 |
| `team()` | 列出团队：每个人的开关、在干活还是在等、跑了几轮、归谁管，还能再招几个 |
| `connect(source, target, back?)` | 在自己（`me`）和下属之间接线，写成 `名字.端口`；闭合成环的线要写 `back=true`。碰不到团队以外的节点 |
| `fail(reason)` | 认为工作做不成了，让整次运行以这个理由失败 |

- **各自的目录：** 有工作区时，用描述写的下属，目录放在上级的目录下面（`lead/lead-r1/`），它只能写自己的目录和共享目录，上级能读到。
- **底下是什么：** 这些都是 `ctx.change`：先检查，再生效，写进日志，接上运行时重做。下属记得自己是谁按哪个模板招的（描述里的 `made_by`），恢复时由招它的 agent 按同一个模板再造一次，所以在 Python 里用 `Engine(..., journal=...)` 也不需要给 `rebuild`。新加的 `Change` 操作有两个：`ports(节点, inputs=, outputs=)`，在运行中给已有节点加端口（加的端口不做类型检查，节点通过 `Node.add_ports` 得知）；`manage(上级, 下属...)`。整个变化被拒绝时，加上的端口也会退掉。
- **下属失败：** 能招人的 agent 会接手下属的失败（见“每个节点的开关”里的“下属失败交给上级”）。`wait` 里会看到一条 `[failed]`，写着谁、哪一轮、什么错，它可以用 `control` 重启、`dismiss` 解雇，或者 `fail` 放弃。
- **还是只走连好的线：** 招人只是多了节点和线，没有“按名字给谁发消息”。

[examples/graphs/hired_team.py](../examples/graphs/hired_team.py)：图里只有一个主控。它招两个数词的下属、各派一半、等两份结果、看一眼团队、解雇两人、交出总数；第三次招人会被拒绝。

## 方法

方法接受节点，返回一张图。运行规则已经在图里：每个节点何时触发，每条回边最多几轮。

| 方法 | 结构 |
|---|---|
| `Chain(a, b, c)` | a → b → c |
| `Loop(a, b, until=f, max_rounds=N)` | 链的末端回到开头，直到 `f` 成立或轮数用完 |
| `Fan(a, b, c)` | 同一输入给各分支，结果汇合。`split` 或 `join` 的端口按分支的 id 命名时（id 里的 `-` 写成 `_`），每个分支拿到和交回的是自己那一份；`split` 有多个输入、`join` 有多个输出时，它们各自成为图的输入和输出 |
| `Tree(lead, a, Tree(b, b1, b2), max_rounds=N)` | 协调者把任务交给下级，下级的回复回到协调者，可以多层 |
| `Star(hub, a, b, c, max_rounds=N)` | 一层的树 |
| `Governed(lead, a, b, operator=person, max_rounds=N)` | 树，加上协调者放行前向操作者节点请求同意 |
| `Pool(lead, member, size=10, max_rounds=N)` | 协调者按需调出成员，最多 `size` 个 |

`Pool` 用在“最多要十个、通常只用三四个”的场合。`member(id)` 是造一个成员的函数，图里有 `size` 个位置，但位置在协调者把活交给它之前什么都不做：不激活，不请求模型，没有对话，日志里也没有它。协调者从 `send` 发出 `{"to": "几何", "task": ...}`：没用过的名字调出一个新成员，用过的名字找到原来那个成员，映射里其余的内容（比如角色）随任务一起交给成员，所以一个模板可以做出干不同事情的成员。不带 `to` 的内容每次调出一个新成员。回复以 `{"from": 名字, "reply": ...}` 回到协调者；位置用完时新名字得到 `{"from": 名字, "error": ...}`。`Pool` 可以放在 `Tree` 的下级位置，也可以放在 `Governed` 里。

自己的方法继承 `Method`，实现 `build(protocols)`。

## 块

节点是某张图里的一个东西，有自己的名字和状态，所以同一个节点不能出现在两个地方。块是设计本身：一份每次都重新造出图来的配方，每用一次得到一个独立的节点。

```python
from interlacing import Block, seq, par

review = Block(lambda: Loop(writer(), critic(), max_rounds=3), name="review")

Chain(outline, review("first-pass"), review("second-pass"), publish)     # 用两次，各有各的状态
Fan(*review.times(4))                                                    # 一次造四个：review-1 到 review-4
```

块的一次使用对外只露出里面那张图的输入口和输出口。

把块拼成更大的块，不用给每个节点起名字：

```python
pipeline = seq(clean, review, publish)             # 一个接一个，是一条链
panel = par(review, review, review)                # 同一份输入给每一个，输出汇成列表
study = seq(clean, par(review, review), publish)   # 可以嵌套
```

`seq` 和 `par` 的每一部分可以是一个块，也可以是一个“给名字就造出节点”的函数。直接放一个已经造好的节点也行，但这样拼出的块只能用一次，因为那个节点没法再造一个。

块里面的运行属于外面的运行：

- 外面有日志时，里面的运行在旁边记自己的日志（`<日志>.d/<节点>-<轮次>.jsonl`）。中断后续跑，块从里面断掉的地方接着跑，已经完成的节点不重做。块的那次激活记入外面的日志之后，里面的日志就删掉。
- 监听口会收到里面的事件，事件上的 `within` 写着它在哪个块里，嵌套几层就有几个名字。
- 时间上限按里面的每个节点计，不按整个块计。
- 里面发现的输出问题也记在外面，节点名写成 `块/节点`。
- 块里可以有人：运行停在等待状态，回答之后从块里面继续。

块也可以存成一个图文件，被别的图引用，这样一个设计可以给很多图用：

```json
{"method": "fan", "nodes": [
  {"id": "review", "kind": "block", "file": "blocks/review.json", "times": 4,
   "defaults": {"agent": {"model": "anthropic:some-model"}}}
]}
```

`file` 是块的图文件，从 `Catalog(files=...)` 允许的目录里读。`defaults` 写在使用处时，会盖过块文件里自己的 `defaults`，所以同一个设计可以换一套设置来用；块里节点自己写明的设置不受影响。`times` 对任何节点都可以用，一次造出 `名字-1` 到 `名字-n`。在 Python 里，`interlacing.spec.block("blocks/review.json", catalog)` 把文件读成一个 `Block`。

节点自己也可以在一次激活里跑一张图：`ctx.engine(graph, protocols).go(inputs)`，上面这些性质都由引擎给出，`GraphNode` 就是这样实现的。

## 运行中增长

要做的事到运行中途才知道时，有两条路：改这张图本身（`ctx.change`），或者让一个节点把需要的图写成数据，交给 `SpawnNode` 在一次激活里跑完。

### 改这张图：`ctx.change`

图在运行中只能用一个显式的操作改变。节点在激活里交出一个 `Change`：

```python
from interlacing import Change

def plan(ctx, task):
    change = Change()
    for i, part in enumerate(split(task)):
        change.add(make_solver(f"s{i}")).connect("plan.y", f"s{i}.x").output(f"s{i}", f"s{i}.y")
    ctx.change(change)
    ctx.emit("y", task)
```

- 操作有 `add(节点...)`、`remove(节点 id...)`、`connect(来处, 去处, 协议="value", back=, max_rounds=, challenges=)`、`disconnect(边 id...)`、`output(名字, 来处)`、`ports(节点, inputs=, outputs=)`、`manage(上级, 下属...)`，按顺序套到当前的图上。删节点时，它身上的边和图输出一起删掉。
- 套上之后按建图的全部规则检查。不通过时 `ctx.change` 抛出 `GraphError`，图一点都不变，节点可以接住它换一种做法；通过了就立刻生效：新节点开始按自己的触发方式运行，新边开始投递，删掉的边上还在等的信封被丢弃，数目报给观察函数。
- 不能删自己，也不能删正在运行的节点。`Engine(max_nodes=N)` 给图能长到多大设上限。
- 观察函数收到 `changed`（`node`、`round`、`added`、`removed`、`dropped`、`graph` 新摘要）；`Engine.graph` 是当前的图，`Result` 里有新节点的轮次和新的图输出。
- 有日志时，改动作为普通数据和提出它的那次激活一起写进日志；恢复时按原来的顺序重做，所以长出来的图在恢复后还在。日志里新节点存的是它的描述，恢复时要把描述变回节点：`Engine(..., rebuild=函数)`，例如 `lambda d: load_node(d, Catalog(allow=[...]))`；运行目录（`Run`、命令行、MCP 服务）自动用图文件的目录做这件事。没有 `rebuild` 时，有日志的运行只能加 agent 招来的下属（它们由招它们的 agent 按模板再造），不能加别的节点。块和 `GraphNode` 不能这样加进去，因为它们的描述里没有里面那张图。

两者的区别：`ctx.change` 改的是这张图，新节点和别的节点一样一直在，可以被反复找到、可以连到已有的节点上；`SpawnNode` 跑的图只活一次激活。

### 跑一张新图：`SpawnNode`

让一个节点把需要的图写成数据，交给 `SpawnNode`：它按描述建图、运行，再把结果发回去。新图只存在于这个节点的一次激活里，和块的里面一样，外面的图一个节点也不多。

```python
from interlacing.spec import Catalog, SpawnNode

grow = SpawnNode("grow", Catalog(allow=["my_steps"], models=models), max_nodes=20)
graph = Chain(planner, grow).build(DEFAULT)
```

`request` 端口收 `{"graph": 图的描述, "inputs": {...}}`，描述的写法和图文件完全一样，方法、`defaults`、`times`、块都能用。`result` 端口发出 `{"outputs": {...}, "rounds": {...}, "problems": [...]}`。用调用边接上时它就是一个工具：智能体带着一张图调用它，拿到结果作为回答，所以协调者可以自己决定这一次派几个成员、怎么连。

新图能碰到什么，由建这个节点时给它的 `Catalog` 决定，不由写图的一方决定：只能用目录允许的函数、模型和程序，文件只能来自目录允许的地方。新图里的节点不能给自己比外面更长的时间上限，新图里再嵌的 `SpawnNode` 和块也不能要比这个节点更多的节点数和线程数。另外有三个上限：

| 设置 | 含义 | 默认 |
|---|---|---|
| `max_nodes` | 一张图最多多少个节点，块里面的也算 | 50 |
| `depth` | 新图里还能再嵌几层 `SpawnNode` | 0，不允许 |
| `max_runs` | 外面一次运行里最多跑几张图 | 不限 |

要把它交给智能体当工具，用 `grow.tool()`：返回的 `Tool` 的说明里已经带着图的格式（`GUIDE`）和这个节点的目录允许的函数及其签名，模型不需要别的提示就能写出图：

```python
agent = AgentNode("designer", model, tools=[grow.tool()])
graph = GraphBuilder().add(agent, grow).connect("designer.grow", "grow.request", "call")...
```

描述建不出来、超过上限、或者新图运行失败时，默认把 `{"error": ...}` 作为结果发回去，发送方可以换一种写法再试；智能体会在工具回答里看到原因。`strict=True` 时改成让这个节点失败，整次运行随之失败。

新图的运行和块一样进日志、能恢复、受时间上限约束，观察函数收到的事件带 `within`。新图里有人要回答时，问题的编号带上所在的位置，例如 `grow.1__first-1` 是 `grow` 第 1 次激活里的 `first` 第 1 次提问，所以不同图里同名的人不会混在一起。

写在图文件里是 `{"id": "grow", "kind": "spawn", "max_nodes": 20}`，它用的就是读这个文件的那个目录。

### 图里有图的几种办法怎么选

这几种办法只在两件事上不同：里面的结构什么时候定下来，里面的东西活多久。

| | 结构何时确定 | 活多久 | 用在 |
|---|---|---|---|
| `GraphNode` | 建图时 | 每次激活跑一遍里面的图 | 把一张现成的图当一个节点；其余三种都建立在它或同样的机制上 |
| `Block` | 建图时 | 同上，每次使用各有自己的状态 | 同一个设计要用多次、要拼接 |
| `Pool` | 建图时留好位置 | 整次运行，成员记得自己的对话 | 成员数目不定，但同一个成员要被反复找到 |
| `SpawnNode` | 运行时 | 一次激活，用完就没有了 | 连结构都要到运行中才知道 |
| `ctx.change` | 运行时 | 整次运行，和建图时就有的节点一样 | 结构到运行中才知道，而且新节点之后还要被找到、和别的节点连着 |

`Pool` 的位置不回收是有意的：位置上的成员带着自己的记忆，把位置让给新名字等于让新成员接着旧对话干活。需要“用完就走、下次是全新的”的成员，用 `SpawnNode`；需要“回头还能找到同一个”的成员，用 `Pool`。

## 引擎

`Engine(graph, protocols, workers=0, journal=None, transport=None, time_limit=None, observe=None).run(inputs)` 返回 `Result`：每个图输出收到的全部内容、各节点运行次数、达到上限的回边、是否做完。

调度是事件驱动的。每条边有自己的信箱，每个节点记着还缺几个必需输入；一个信封到达只改动一个节点的计数，所以每条消息的调度开销与图的大小无关（实测 10 到 10,000 个节点都是每个信封约 9 微秒）。`workers=0` 在调用线程里按固定顺序运行；更多工作线程时节点并行运行，同一个节点不会同时运行两次。几个线程在只花几微秒的小节点之间来回交接时，每个信封要 60 到 100 微秒，这是 Python 线程交接的代价，对一次要几秒的模型调用可以忽略。协程节点在事件循环上每次激活约 20 到 30 微秒。

`journal=Journal(path)` 让运行可恢复：每次完成的激活整条写入日志，新引擎用同一个日志 `resume()` 接着运行。做完的不重做，被打断的重跑。`resume(change=True)` 让运行在另一张图下继续。

`time_limit` 是一次激活最长可以用的秒数，节点自己的 `time_limit` 属性优先（`HumanNode` 默认不限时）。线程无法被强行终止，所以超时的处理是：运行以失败结束并指出是哪个节点，其他节点看到 `ctx.stopping()`，引擎不再等卡住的那个节点，它之后做的事全部作废。超时的激活没有写进日志，`resume()` 会重新跑它。

`observe` 是一个函数，引擎把正在发生的事逐件告诉它（交给上级的失败，`failed` 事件里多一个 `to`）：`started`、`emitted`、`corrected`、`answered`（驳回有了答复）、`received`、`saved`、`finished`、`failed`、`stopped`、`used`、`changed`（图在运行中改了）（都带 `node` 和 `round`），`control`（开关被拨，带 `nodes`、`action`，由节点拨时还有 `by`），最后是 `ended`。界面或日志要实时跟随一次运行就挂在这里，不必去读日志文件。它在运行节点的线程里被调用，不应当抛出异常；抛出了，运行会失败并说明原因。

运行被停下时（有节点失败、超时、超预算，或者外面要求停止），正在运行的节点从 `ctx.stopping()` 得知。问过并得到“是”的那次激活可能只做了一半，所以引擎不把它记成已完成：它的输入留在原处，`resume()` 时重跑，观察函数收到的是 `stopped` 而不是 `finished`。没有问过 `ctx.stopping()` 就正常返回的激活照常记为完成。块和 `SpawnNode` 里面的图因此不会在被打断后留下一个“做完了但没有输出”的记录。

`firm=True` 用于图不是自己写的场合：这时 `time_limit` 是硬上限，节点自己写的更长的 `time_limit` 不算数（等人的节点和装着别的图的节点除外，它们里面的节点各自计时）。`SpawnNode` 跑的图总是这样对待。

`budget` 是整次运行的用量上限。节点用 `ctx.use(tokens=1200, calls=1)` 报告自己用掉了什么，名字由节点自己定，引擎只管按名字累加；`AgentNode` 每次调用模型都会报告 `calls`、`input_tokens`、`output_tokens` 和 `tokens`。`Engine(..., budget={"tokens": 200000})` 时，任何一项的累计超过上限，运行就以失败结束并说明用了多少，即使那个节点自己不停下来也一样。块和 `SpawnNode` 里面用掉的算在外面的运行上。累计值在 `Result.used` 里，失败时在 `RunError.used` 里，观察函数会收到 `used` 事件。用量逐条写进日志，所以恢复之后接着累加；被打断后重跑的激活花了两次钱，账上也是两次。超出预算而失败的运行，要给更大的预算才能 `resume()`。

`ctx.key` 是这一次激活的名字，形如 `运行标识/节点#轮次`，块里面的会带上外层的位置。同一次激活在中断后重跑时名字不变，不同的运行名字不同。恢复是“至少一次”，所以节点要对图外做不能重复的事（下单、发消息、写数据库）时，把它作为幂等键交给对方，或者自己先查这个名字是否已经做过。内核只提供这个名字，不替节点去重。

`ctx.once(name, do)` 把一件图外的事做一次：`do()` 的返回值按 `name` 记进日志，这次激活重跑时（运行中断后接着跑，或者发出的内容被拒后重做）直接拿回记下的值，不再调用 `do`。返回值必须是普通数据。进程恰好死在“做完”和“记下”之间时仍会重做，所以最稳妥的写法是两者一起用：`ctx.once("charge", lambda: bank.charge(order, key=ctx.key))`。

节点可以是协程：`run`（还有 `open`、`close`）写成 `async def`，或者交给 `FunctionNode`、`@node` 的函数是 `async def`。图里有这样的节点时，引擎在一个事件循环上运行：协程节点在循环里被 await，不占线程；普通节点照旧放进线程。`workers` 仍然是同时进行的激活数的上限，`workers=0` 时一次一个、顺序固定，所以一百个都在等模型回答的协程节点用 `workers=100` 就能同时等，而不会多出一百个线程。协程节点里的 `ctx.emit`、`ctx.use`、`ctx.change` 照常直接调用；调用边要用 `await ctx.acall(port, payload)`，在协程节点里用 `ctx.call` 去调另一个协程节点会被拒绝。超时的协程节点会被真正取消（它收到 `CancelledError`），不像线程里的节点只能被放弃。

调用方是协程时用 `await engine.arun(inputs)`、`aresume()`、`ago()`。图里有协程节点时，运行就在调用方的事件循环上进行；没有时运行在线程里，事件循环同样不被占用。取消等待会让运行停下，正在 await 的协程节点被取消。同步的 `run()` 遇到有协程节点的图，会自己开一个事件循环；如果调用它的线程里已经有事件循环在运行，就换一个线程去开。协程节点里跑一张内层的图，用 `await ctx.engine(graph).arun(...)`，它就在同一个循环上运行。

`transport` 是发送和接收之间的接口，进程内不需要它；跨进程或跨机器的传递方式放在这里。

引擎内部按职责分成几个部件，`Engine` 只负责把它们串起来，各部件不对外公开，接口可能变：

| 部件 | 管什么 |
|---|---|
| `wiring` | 图在运行时的样子：每条边的信箱、每个输入口、每个节点的状态，开始运行时建一次 |
| `scheduler` | 信封入箱、取出，节点还缺几个输入，就绪队列 |
| `checks` | 发出时和到达时的检查，退回重做、放行警告、记录问题 |
| `ledger` | 用量的累计和预算，`ctx.once` 记下的结果 |
| `recorder` | 往日志里写什么，以及从日志恢复一次运行 |
| `clock` | 每个节点的时限，以及哪个激活超时了 |
| `context` | 节点拿到的 `ctx` |

## 把图写成数据

`interlacing.spec` 把图写成 JSON，再从 JSON 建回同一张图：

```python
from interlacing.spec import Catalog, dump, load, read, write

write("graph.json", graph)                                   # 图的定义，加上嵌套子图的定义
again = read("graph.json", Catalog(allow=["mypkg"]))         # again.digest == graph.digest
```

写出来的就是图自己的描述（摘要正是由它算出的）：每个节点的种类、端口、配置，每条边，图的输入输出。函数用带模块的名字指明（`mypkg.steps.clean`），所以必须是模块顶层的函数，`lambda` 写不出去。模型用名字指明。

描述里只有名字，建图时要把名字变成真东西，这等于让描述决定运行哪些代码。`Catalog` 是宿主划定范围的地方，默认什么都够不到：

| 参数 | 含义 |
|---|---|
| `allow` | 函数可以来自哪些模块，`"mypkg"` 包括它下面的所有模块 |
| `processes` | 是否允许 `process` 节点，它会在本机启动程序 |
| `models` | `models(name)` 返回智能体用的模型；不给就建不了智能体 |
| `ask` | `ask(node_id, round, request)` 替图里没有自带函数的 `human` 节点回答 |
| `protocols` | 编译图用的协议表 |
| `files` | 描述可以从哪个目录取文字；不给就不读任何文件，目录之外的文件一律拒绝 |
| `defaults` | 宿主给每种节点的默认设置 |

`catalog.register(kind, loader)` 加入自己的节点种类。

`allow` 只放行在允许的模块里**定义**的函数（类、带 `__call__` 的对象按它的类定义在哪里算）：`mypkg.os.system` 这种借允许的模块去够它导入的别的模块的写法会被拒绝，从别处导入进来的函数也不行（用 `@node` 包过的函数按被包的那个函数算）。模型写的图、MCP 客户端送来的图都受这条约束。

### 格式说明：`GUIDE` 和 `functions`

`interlacing.spec.GUIDE` 是一页纸的图格式说明，写给第一次写图的人和模型看；`functions(catalog)` 列出目录允许的函数，每个带签名和文档的第一行；`guide(catalog)` 是两者合在一起。MCP 服务有同名的 `guide` 工具，命令行是 `interlacing guide --allow mypkg`，`SpawnNode.tool()` 的说明里也是它。

`GUIDE` 里还列出常用节点种类的端口（`human` 是 `request` 到 `answer`，`branch` 是 `x` 到 `yes`/`no`，等等），测试会核对它和真实端口一致。写图时常错的地方，`GUIDE` 里都写了：边写成 `{"from": "a.out", "to": "b.text"}`；方法的图输入叫 `in`、输出叫 `out`；fan 的输出是各分支输出的列表；方法里的节点可以直接是另一个方法。写错形状时，报错会说出正确的写法；端口或图输入写错时，报错会列出有哪些。

### 只写不同的地方

搭自己的框架时，图文件里只需要写结构，和你不想用默认值的那几项。

`defaults` 给一种节点统一的设置，节点自己只写不同之处。优先顺序是：节点自己写的，然后是描述里的 `defaults`，然后是 `Catalog(defaults=...)`，最后是包的默认值。

任何一项设置都可以不写在描述里，而是指向描述旁边的一个文件：`{"file": "prompts/lead.md"}`。想换提示词，就写一个文件，再这样引用它。命令行默认允许读图文件所在目录里的文件。

每个节点可以带 `time_limit`（秒），也可以放进 `defaults`。

```json
{
  "defaults": {"agent": {"model": "anthropic:some-model", "max_steps": 20, "time_limit": 600}},
  "method": "pool", "size": 10, "max_rounds": 3, "places": "scout",
  "root": {"id": "lead", "kind": "agent", "system": {"file": "prompts/lead.md"}, "firing": "any",
           "inputs": ["task", {"name": "reply", "required": false}], "outputs": ["out", "send"]},
  "member": {"kind": "agent", "system": {"file": "prompts/scout.md"}}
}
```

端口的检查在图文件里这样写，检查函数和别的函数一样用带模块的名字：

```json
{"name": "chart", "schema": {"file": {"suffixes": [".png"], "max_size": 5000000}}, "check": "mypkg.checks.has_axes",
 "invalid": "warn", "repairs": 2}
```

`dump` 写出来的东西里不再有 `defaults` 和文件引用，默认值和文件的文字都已经落在各个节点上，所以运行目录里保存的图不依赖原来的文件。

在 Python 里不需要这些：提示词就是字符串，`AgentNode("lead", model, system=Path("prompts/lead.md").read_text())`。

手写时可以更短：节点的设置直接写在 `id`、`kind` 旁边，端口只写名字，边只写 `from` 和 `to`；也可以不写节点和边，直接指定方法：

```json
{"method": "loop", "max_rounds": 4, "until": "mypkg.checks.good_enough", "nodes": [
  {"id": "draft", "kind": "agent", "model": "anthropic:some-model", "system": "..."},
  {"id": "tidy", "kind": "function", "function": "mypkg.steps.tidy"}
]}
```

函数节点和在 Python 里一样从函数读端口：每个参数是一个输入，输出叫 `out`，第一个参数叫 `ctx` 时它是上下文而不是输入。只有要改名或加检查时才需要写 `inputs`、`outputs`（或 `"context": true/false`）。

方法里的节点可以直接写成另一个方法，它会变成一个装着那张图的节点，名字是它的 `id`，没写就用方法名（`fan`、`fan_2`……）。和写 `{"kind": "graph", "graph": {...}}` 是一回事：

```json
{"method": "chain", "nodes": [
  {"id": "clean", "kind": "function", "function": "mypkg.steps.clean"},
  {"method": "fan", "nodes": [{"id": "count", "kind": "function", "function": "mypkg.steps.count"},
                              {"id": "top", "kind": "function", "function": "mypkg.steps.top"}]},
  {"id": "report", "kind": "function", "function": "mypkg.steps.report"}
]}
```

## 命令行和运行目录

安装后有命令 `interlacing`（也可以用 `python -m interlacing.cli`）。每个命令都输出 JSON。

| 命令 | 作用 |
|---|---|
| `guide [--allow 模块]...` | 打印图的格式说明，和这些模块允许图使用的函数（这一条输出文字，不是 JSON） |
| `check graph.json` | 只建图不运行，输出摘要、节点、输入输出；不需要密钥 |
| `run graph.json --dir RUN` | 在新目录里开始一次运行；`--input 名=值`（值按 JSON 读，读不成就当文字）或 `--inputs 文件`；`--time-limit 秒` 是一次激活的时间上限；`--budget 名=数`（可以重复）是整次运行的用量上限，例如 `--budget tokens=200000`；`--workspace 目录` 是节点的工作目录，不给就是运行目录里的 `files/` |
| `resume --dir RUN` | 沿用开始时给的范围（`--allow` 等不用再写），从日志接着跑：等人回答之后、失败修好之后、被中断之后都用它；`--budget 名=数` 换一个更大的预算再继续 |
| `answer --dir RUN [问题] --json 值` 或 `--text 文字` | 回答图里的人被问到的问题；只有一个待答问题时不用写问题的名字。回答后运行还停着，输出里的 `next` 提醒要再 `resume` |
| `status --dir RUN` | 输出运行的状态和待答的问题 |
| `files --dir RUN` | 列出工作区里的文件，以及每个文件是哪个节点在第几次激活里写的 |
| `control 动作 节点... --dir RUN [--team]` | 暂停、恢复、停止或重启节点，见“每个节点的开关，以及谁管谁”；运行没在进行时写进日志，下次 `resume` 生效 |
| `node-serve 模块:名字` | 把一个节点放在这个进程里服务，供别处的 `RemoteNode` 使用；`--listen 地址:端口`、`--token-env 变量名`、`--make`，见“在别的进程或别的机器上的节点” |

命令所在的目录会加进模块搜索路径（和 `python -m` 一样），所以旁边的 `steps.py` 可以直接 `--allow steps`。输出被提前关掉时（`| head`）不算出错。

`check` 和 `run` 用 `--allow 模块`（可以重复）、`--processes`、`--tools 工具池目录`、`--apps 应用目录` 和 `--remote 地址` 划定图能够到的范围，和 `Catalog` 的含义相同；不给就什么都够不到。范围在 `run` 时写进运行目录，`resume` 沿用它。`--workers N` 是并行运行节点的线程数。

`run` 和 `resume` 的退出码说明运行停在哪里：0 完成，1 失败或被拒绝，3 在等人回答，4 因为轮数或停止条件而停下。

一次运行的全部内容都在它的目录里，所以可以由一个进程开始、另一个进程回答、第三个进程继续：

```
graph.json      图的定义
options.json    图能够到的范围、线程数、时间上限和预算
inputs.json     开始运行时给的输入
journal.jsonl   已经发生的事；块和运行中长出的图另有自己的日志，在 journal.jsonl.d/ 下，所在的激活完成后删除
questions/      图里的人被问到的问题，一个问题一个文件，名字是 <节点>-<轮次>
answers/        已经给出的回答
state.json      运行的状态：done、waiting、stopped、failed，以及检查发现的问题
files/          节点的工作目录，file 协议的路径相对于它
lock            有进程正在运行这张图时存在，里面写着是哪个进程
```

图文件里的图总是带着 `file` 协议，工作目录就是上面这个；`process` 节点默认在这里启动，智能体能看到这里的图片。`status` 的输出里有 `problems`，列出每个没通过检查的输出和它的结局，还有 `used`，是到目前为止的用量。

一个运行目录同一时间只由一个进程运行。第二个进程来 `run` 或 `resume` 会被拒绝，并被告知是哪台机器上的哪个进程从什么时候开始占着；`status` 和 `answer` 不受影响。占着的进程如果已经死了（同一台机器上能查出来），锁会被自动接管；在另一台机器上查不出来，就要人确认后删掉 `lock` 文件。

图里没有自带函数的 `human` 节点由运行目录代答：回答还没有时，运行停在 `waiting`，进程退出，不会有进程挂着等人；`answer` 写下回答，`resume` 继续，已经完成的节点不会重跑。

模型的名字写成 `提供方:模型`，密钥只从环境变量读取，不写进图文件、运行目录或日志：`anthropic:` 用 `ANTHROPIC_API_KEY`（可选 `ANTHROPIC_BASE_URL`），`openai:` 用 `OPENAI_API_KEY` 和 `OPENAI_BASE_URL`（任何兼容接口），`fake:echo` 不调用任何服务，原样返回收到的话，用来试图的接法。

在 Python 里用 `interlacing.runs.Run` 做同样的事：`Run.create(目录, 描述, allow=...)`、`start(inputs)`、`resume()`、`questions()`、`answer(值)`、`status()`。

## MCP 服务

`interlacing serve --runs 目录 [--allow 模块]... [--processes] [--workers N] [--time-limit 秒] [--files 目录] [--tools 目录] [--apps 目录] [--remote 地址]... [--budget 名=数]...` 把同样的操作通过 Model Context Protocol 提供给别的应用（通常是那个应用里的模型）。传输是 stdio，一行一条 JSON-RPC 消息。在支持 MCP 的应用里这样配置：

```json
{"mcpServers": {"interlacing": {"command": "interlacing", "args": ["serve", "--runs", "/path/to/runs", "--allow", "mypkg"]}}}
```

| 工具 | 参数 | 作用 |
|---|---|---|
| `guide` | 无 | 图的格式说明，和这个服务允许图使用的函数（带签名）；客户端写图之前先读它 |
| `check` | `graph` | 只建图不运行，返回摘要、节点、输入输出 |
| `start` | `graph`、`inputs`、`wait` | 开始一次运行，返回运行的名字和状态。`wait: false` 时立即返回 `running`，运行在服务里继续 |
| `status` | `run` | 运行的状态和待答的问题 |
| `resume` | `run`、`wait` | 从日志接着跑，`wait` 同上 |
| `stop` | `run` | 让正在进行的运行停下，之后可以再 `resume` |
| `control` | `run`、`nodes`、`action`、`team` | 暂停、恢复、停止或重启其中的节点；运行正在进行时（在这个服务里或别的进程里）片刻之内生效 |
| `answer` | `run`、`answer`、可选 `question` | 写下人对问题的回答，之后要再调 `resume` |
| `runs` | 无 | 服务手里的所有运行和各自的状态 |

客户端是不被信任的一方：

- 图能够到哪些模块、能不能启动程序、用几个线程，都由启动服务的人在命令行上定。客户端在调用里多写什么都不起作用，默认什么都够不到。
- 运行放在哪里由服务决定，名字由服务生成；客户端给的名字只用来在 `--runs` 目录下查找，带路径的名字一律查不到。
- 图里的 `{"file": ...}` 只能读 `--files` 目录下的文件，不给这个参数就一个文件都读不了。
- 工具文件只能来自 `--tools` 目录，当作节点的应用只能是 `--apps` 目录下的文件夹。
- 模型的密钥来自服务进程的环境变量，客户端看不到，也传不进来。
- 节点打印的东西去标准错误，标准输出上只有协议消息。

每次运行就是 `--runs` 下的一个运行目录，和命令行用的是同一种，所以服务开始的运行可以在终端里用 `status`、`answer`、`resume` 查看和继续。

客户端送来的图按不可信对待：节点不能给自己比 `--time-limit` 更长的时间，块不能要比 `--workers` 更多的线程（`Run.create(..., strict=True)`）。

工具调用并行应答，一个长时间的运行不会挡住对别的运行的询问。`start` 和 `resume` 默认持续到运行完成、需要人回答或者失败；带 `wait: false` 时立即返回，运行在服务里继续，用 `status` 查询，用 `stop` 提前结束。图里的人需要回答时，`start` 或 `resume` 返回 `waiting` 和问题，由客户端把问题交给人，拿到回答后调 `answer` 再调 `resume`。回答是否真的来自人，服务无法分辨，这要由接入它的应用来保证。

## 示例

[examples/data/](../examples/data/) 是写成 JSON 的图和它们用到的函数，用命令行运行。`release.json`：起草、等编辑同意、发布。`scouts.json`：一个协调者和十个位置的 `Pool`，默认设置写在 `defaults` 里，成员的提示词来自 `prompts/scout.md`，给三个方向就只有三个成员运行。

[examples/graphs/](../examples/graphs/) 下有十九个可以直接运行的示例：只用普通函数写的图、一个主控带两个常驻 agent、主控在运行中招人组队、先发粗估再更正、审阅驳回计划、点名驳回几个估计中的一个并连带更正一起发出的说明、放在别的进程里一直开着的节点、运行中给自己加节点的图、把一个文件夹（[examples/apps/word_count](../examples/apps/word_count/)）当作一个节点、在工作区里自由干活并使用工具池的 worker、块门口的收发台、链里接一个外部程序、写作和评审的循环、需要人同意的树、相机到机械臂、按路径传文件并纠正一次缺失的文件、把设计做成块再拼起来用、运行中按任务写出一张图并运行、模型做空间规划而控制器执行。三个更完整的案例（物理智能体、建模助手、并行探索加验证器）在 [CASES.md](CASES.md)。

## 限制

- 恢复是“至少一次”：被打断的节点会重跑。`ctx.once` 让图外做过的事不再重复，只有“做完”到“记下”之间的一瞬间例外，那一瞬间靠把 `ctx.key` 交给外部服务去重。
- 流和调用还不经过传输接口；目前只有进程内的实现。远端节点（`RemoteNode`）只能发普通的值，不能接流或调用，也不能在远端调用图里的节点。
- 设备节点只用假设备测试过。两种模型接口各对一个真实的兼容服务做过一次很小的验证（普通回答、流式、图里的工具调用、JSON 输出、密钥错误、命令行跑一条链），自动测试仍然只用脚本化的假模型；思考内容、图片输入、限流重试没有对真实服务验证过。
- 运行目录的锁只在 `Run`、命令行和 MCP 服务这一层；直接用 `Engine` 和 `Journal` 时，同一个日志文件不能给两个引擎同时用。Windows 上不会自动接管死进程留下的锁。
- 测试在 Linux、macOS 和 Windows 上都跑。Windows 上外部程序输出的 `\r\n` 会换成 `\n`；控制台不是 UTF-8 时，命令行输出的 JSON 里非 ASCII 字符写成转义。
- 预算在节点报告用量的那一刻检查，所以会超出一次调用的量；不报告用量的节点不受预算约束。
- MCP 服务只有 stdio 一种传输。工具调用并行应答；客户端取消一次调用（`notifications/cancelled`）会停下那次调用等着的运行；客户端断开时，还在进行的运行被停在可以接着跑的地方。测试里由参考客户端（`mcp` 包）跑通过，还没有接过桌面应用。
- 在引擎自己进程里超时的同步节点只是被放弃，线程无法强行终止，要等它自己返回才结束；协程节点会被取消。`ProcessNode` 和 `IsolatedNode` 不在此列：运行一停下（包括超时），它们的进程就被杀掉。自己写的长时间节点应当定期看 `ctx.stopping()`，或者放进 `IsolatedNode`。
- `Pool` 的位置在一次运行里不回收（原因见“图里有图的几种办法怎么选”）。用 `ctx.change` 在运行中加的节点，恢复时靠 `rebuild` 从描述再造出来，所以不能是块或 `GraphNode`。
- 检查只在内容发出和到达的时刻进行；文件在两次检查之间被改动，要到下一次到达才会发现。`hash=False` 时只比较大小。
- 引擎重跑一次激活时，节点在上一次尝试里对图外做过的事（写过的文件、调用过的工具）不会撤销。
- 块里面的运行进了日志之后，块里的边上也只能传普通数据。
- `SpawnNode` 跑的图在那次激活结束后就不在了，不能在之后的轮次里接着给它发消息；需要长期存在的成员用 `Pool`。
- 更正按 `rerun` 重跑的是节点的 `run`：节点自己存着的状态（比如累加的计数）不会先退回去，有这类状态的节点应当用 `tell` 或 `ignore`。
- 一个节点失败会让整次运行失败，除非它的上级 `oversees` 并且正在运行（见“下属失败交给上级”）。交给上级的失败不写进日志：运行接上时，失败的那次激活重跑。
- 解雇一个还在干活的下属时，引擎最多等它 5 秒停下再拿出图；还没停下的，上级在下次调用模型前再拿一次。上级在那之前就结束的话，下属以“已停止”的状态留在图里，不再做任何事。
- `max_nodes` 限制的是一张图的大小，不是花费；模型调用的总量要靠 `max_runs`、各方法的 `max_rounds` 和时间上限一起约束。
