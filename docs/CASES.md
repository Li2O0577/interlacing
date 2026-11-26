# 三个案例：这个内核在实践里怎么用

[English summary](../README.md#what-it-is-for) · [完整说明](GRAPH.md) · [返回](../README.zh-CN.md)

三个案例都按同一个办法读：先看要做的系统是什么形状，再看每一块落在内核的哪个部件上，最后看哪些事内核不管、要自己写。案例里的代码都用假模型跑通过；第一个案例整个是可以运行的示例。

内核只做一件事：按图把信息从一个节点带到另一个节点，并且把这个过程管好（检查、记账、限时、可恢复、可观察）。模型怎么想、机械臂怎么动、论文怎么排版，都是节点里面的事。

## 一、模型做空间规划，控制器执行：类似 Q0-Preview 的物理智能体

### 原系统的形状

[Q0-Preview](https://github.com/XPolicyLab/XPolicyLab/tree/main/policy/Q0_Preview) 的 README 这样描述自己（下面只依据它的 README 和目录结构，没有读它的实现）：

```text
评测器的观测 → 适配层 → 回合框架 ⇄ 模型 API
                              ↓
                        校验过的动作组
                              ↓
评测器的关节动作 ← 运动规划和执行
```

模型不直接输出关节角，而是用几个世界坐标系里的点表达夹爪位姿；模型可以调用 Python 工具做几何计算；一个计划是一到两个动作组，两组之间有一次视觉检查，决定继续还是重新规划；采纳的计划带一小段记忆进入下一轮；有计划次数、整回合时长、单轮时长、检查时长四种预算；全部请求和工具产物归档。

### 落在内核上

| Q0-Preview 里的东西 | 内核里的部件 |
|---|---|
| 观测进入、规划、执行、再观测的循环 | `Loop(observe, planning, act, until=完成, max_rounds=max_plans)` |
| 模型 API 和工具循环 | `AgentNode`，工具是调用边另一头的普通节点 |
| 隔离的 Python 工具 | `ProcessNode`（另起进程）或 `FunctionNode`，用 `call` 边接到模型的工具上 |
| 相机图像给模型 | 观测节点把图像写进工作目录，用 `file` 协议传相对路径；`AgentNode` 把路径指向的图片作为图片交给模型 |
| “校验过的动作组” | 规划输出端口上的 `check=valid_plan, repairs=2`：不合约定的计划连同原因退给模型改，改不好就失败，没通过的计划到不了控制器 |
| 两个动作组之间的视觉检查 | 再接一个 `AgentNode` 和 `Branch`：继续走 `yes`，重规划走回边 |
| 采纳的计划带短记忆、有界的历史窗口 | `AgentNode(memory=函数)`，函数决定每次请求前保留哪些消息 |
| `max_plans`、`wall_seconds` | `Engine(budget={"plans": 100, "seconds": 10800})`，执行节点用 `ctx.use(plans=1, seconds=耗时)` 报告 |
| `turn_seconds`、`checkpoint_seconds` | 引擎的 `time_limit` 和检查节点自己的 `time_limit = 60` |
| 每回合的请求、响应、工具产物归档 | `Journal` 记下每次激活的输入输出；`observe` 回调实时拿到每个事件；文件留在运行目录的 `files/` |
| 运动规划、碰撞检测、25 Hz 关节控制 | 不在内核里。它们在执行节点内部，内核只把通过检查的动作组交给它 |

可以运行的版本在 [examples/graphs/planner_and_arm.py](../examples/graphs/planner_and_arm.py)，机器人、模型、运动规划都是替身，内核做的事是真的：

```python
planner = AgentNode("plan", model, json=True, memory=recent, max_steps=8,
                    system="Give the gripper pose as three world-frame points. Measure before you plan.",
                    tools=[Tool("measure", "Measure the scene from the current images.")],
                    outputs=[Port("plan", dict, check=valid_plan, repairs=2)])      # 动作约定在这里检查
planning = (GraphBuilder().add(planner, FunctionNode("measure", measure, ...))
            .connect("plan.measure", "measure.request", "call")                      # 工具是图上的节点
            .input("in", "plan.task").output("out", "plan.plan").compile(DEFAULT))

graph = Loop(observe, GraphNode("planning", planning, DEFAULT), act,
             until=lambda seen: at_goal(seen), max_rounds=10).build(DEFAULT)
Engine(graph, DEFAULT, time_limit=60, budget={"plans": 10, "seconds": 600}).run({"in": "start"})
```

运行它会看到：模型先调用测量工具；第一份计划里两个参考点相距 200 毫米，违反 5 到 85 毫米的约定，被退回并告知原因；模型改正后夹起方块；第二轮规划给出两个动作组把方块放到标记上。

### 能不能做到，差在哪

规划这一层可以完整做出来，上表里每一行都有对应部件，没有要改内核的地方。三点需要说清楚：

1. **25 Hz 的控制回路不走图**。引擎调度一个信封约 11 微秒，频率上不是问题，但关节级控制应当是执行节点内部的紧循环，图只在“秒”这一级工作。原系统也是这样分层的。
2. **谁是主循环**。评测器按步来要动作，是它在驱动。接法是适配层在自己的线程里跑图，用 `SourceNode` 从队列读观测、`SinkNode` 往队列写动作组；评测器每来一步，适配层从队列里取当前该执行的关节动作，没有新计划时保持位置。这一层几十行，属于适配层，不属于内核。
3. **没有用真实机器人和真实多模态模型验证过**。图片输入的代码路径只用假模型测过。

## 二、数学建模助手：主控、分题负责人、质疑者、人随时介入

### 要做的系统

一道赛题有若干小问，按顺序做。每个小问一个负责人，带着求解和质疑两方反复讨论，直到答案有证明；主控只给目标不规定交付形式；人可以在任何一步介入；最后汇总成论文。之前实测暴露过的要求是：优化问题必须给出精确值或者紧的上下界加证明，数值搜索出来的“差不多”不算完成；派发小问时只给题目文件，不给前面小问的全文。

### 落在内核上

| 要求 | 部件 |
|---|---|
| 每个小问是同一套做法 | `Block`：把“求解、质疑、回到求解”做成一个设计，每个小问用一次，各有自己的对话 |
| 求解和质疑反复讨论 | `Loop(solve, devil, until=质疑者接受, max_rounds=4)` |
| “没有证明不算完成” | 求解输出端口上的 `check`；是失败、是警告放行、还是退给模型改，由 `invalid` 和 `repairs` 决定 |
| 结果要程序复核 | 求解之后接一个跑校验程序的节点。校验不过要作为结论送回去讨论，就用函数节点包一层；直接用 `ProcessNode` 时非零退出码会让整次运行停下 |
| 小问按顺序做 | `seq(q1, q2, q3)`，或者主控一次派一个 |
| 只给题目文件 | `file` 协议传相对路径，提示词里约定各自能读的目录；内核不做文件权限 |
| 主控带若干负责人 | `Tree` 或 `Pool`；小问数目到读完题才知道，用 `Pool`（位置预留，用到才出现）或 `SpawnNode`（结构也到运行时再定） |
| 人随时介入 | `Governed`：主控通过 `approve` 工具问人；运行目录里人没回答时进程退出，回答后 `resume` 继续 |
| 每个角色用不同的模型 | 图文件的 `defaults` 写默认模型，个别节点自己写 `model` 覆盖 |
| 提示词可换 | `"system": {"file": "prompts/devil.md"}` |
| 花费可控 | `--budget tokens=...`，超出时整次运行停下，加预算后 `resume` |
| 容器重启不丢进度 | 运行目录里的日志；智能体的对话在日志的快照里，恢复后接着原来的对话 |

一个小问的设计（用假模型跑通过）：

```python
question = Block(lambda: Loop(
    AgentNode("solve", model, json=True,
              outputs=[Port("solution", dict, check=needs_proof, invalid="warn", repairs=0)]),
    AgentNode("devil", model, json=True, system=DEVIL_PROMPT),         # 质疑者：最优性证明了吗
    until=lambda verdict: verdict["accepted"], max_rounds=4), name="question")

study = seq(question, question, question, name="study")               # 三个小问依次做
graph = Chain(intake, study("study"), paper).build(DEFAULT)
```

第一次求解给出 111.6 而没有证明时，端口检查记下一条问题（`Result.problems` 里能看到是哪个节点哪一轮），质疑者不接受，回到求解；第二次给出 110.97 和证明，才往下走。

写成图文件时只需要结构和不同于默认的部分：

```json
{"defaults": {"agent": {"model": "openai:some-model", "time_limit": 900}},
 "method": "governed", "max_rounds": 6,
 "root": {"id": "lead", "kind": "agent", "system": {"file": "prompts/lead.md"}, "firing": "any",
          "inputs": ["task", {"name": "reply", "required": false}], "outputs": ["out", "send"],
          "tools": [{"name": "approve", "description": "Ask the operator before going on."}]},
 "children": [{"id": "q", "kind": "block", "file": "blocks/question.json", "times": 3}],
 "operator": {"id": "operator", "kind": "human"}}
```

### 内核不做、要应用自己做的

界面、聊天渲染、公式显示、论文模板和导出、实验结果表格，这些是应用。本地应用可以用 Python 的 `Run` 与 `observe` 回调驱动界面，也可以经 MCP 接入。多位审核人共用一台服务器时，可用 PostgreSQL 协作服务分派和提交任务，主管通过 HTTP 订阅根运行进度。[多人审核示例](POSTGRES.md)。“单个小问不能比直接问模型差”这条要求，内核能做的是不往模型的输入里加东西：系统提示词、历史消息、工具说明都是图里写明的；内核自己只在一个节点有多个输入时标上端口名，在输出没通过检查时把原因告诉模型。

## 三、一位数学家：主控派出多个并行的探索者，各自交给 Lean 验证

### 落在内核上

```python
def lean(proof):                                    # 把证明交给 Lean，报错不算节点失败，而是一份结论
    done = subprocess.run(["lean", "--stdin"], input=proof, capture_output=True, text=True, timeout=300)
    return {"ok": done.returncode == 0, "proof": proof, "errors": done.stdout + done.stderr}

def explorer(id):                                   # 一个探索者：写证明，交给 Lean，不过就按报错再改
    body = Loop(AgentNode("prove", model, system=EXPLORE_PROMPT),
                FunctionNode("lean", lean, inputs=[Port("proof", str)], outputs=[Port("verdict", dict)]),
                until=lambda verdict: verdict["ok"], max_rounds=5)
    return GraphNode(id, body.build(DEFAULT), DEFAULT)

graph = Pool(lead, explorer, size=10, max_rounds=1).build(DEFAULT)
Engine(graph, DEFAULT, workers=4, budget={"tokens": 2_000_000}, journal=Journal("run.jsonl")).run({"in": problem})
```

- 主控发出 `{"to": "归纳", "task": ...}` 就调出一个探索者；给三个方向只有三个探索者运行，其余七个位置不存在。
- `workers=4` 让探索者并行。每个探索者自己的循环在自己的块里，互不干扰。
- 探索者“验证通过后停止并反馈给主控”就是 `Loop` 的 `until`：Lean 通过，结果作为 `{"from": "归纳", "reply": ...}` 回到主控；五轮不过，最后一次结果照样回去，主控能看到它没过。
- 证明文件用 `file` 协议传路径；主控整合文件是主控节点里的事。
- 探索一半断电，`resume()` 之后做完的探索者不重跑，跑到一半的那个从它块里的日志接着跑。

用假模型和假的 Lean 跑过这个结构：三个方向各在第二次尝试通过，主控收到三份结果，共六次模型调用。真实的 Lean 没有接过。这里用函数而不是 `ProcessNode` 调 Lean，是因为 `ProcessNode` 把非零退出码当作节点失败，而“证明没过”在这里是要送回去的正常结果。

## 四、接进已有的应用

三个案例里的图都可以写成 JSON。`interlacing serve --runs 目录 --allow 模块 --budget tokens=...` 把检查、开始、查看、回答、继续这几个操作作为 MCP 工具提供出去，别的应用里的模型自己写图、自己跑。图能够到什么、能花多少、单个节点能跑多久，由启动服务的人在命令行上定，客户端改不了。

## 共同的界线

| 内核管 | 内核不管 |
|---|---|
| 信息按什么协议从哪到哪 | 模型怎么想，提示词写什么 |
| 输出合不合约定，不合怎么办 | 约定本身的内容（那是一个函数） |
| 花了多少、跑了多久、有没有超 | 一次调用值多少钱 |
| 中断后从哪继续 | 节点对图外做过的事要不要撤销（节点用 `ctx.key` 自己去重） |
| 谁在等人回答 | 用什么界面问人 |
| 图里能用哪些函数、模型、程序 | 那些函数自己安不安全（`allow` 是白名单，不是沙箱） |
