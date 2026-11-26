# Interlacing 与 LangGraph 能力复核

2026-11-26。Interlacing 以功能分支的运行时代码 [324dfab](https://github.com/Li2O0577/interlacing/commit/324dfab85e546c6e9467536753cb7044170f70af) 为基线。对照实验使用 LangGraph Python 1.2.24、langgraph-checkpoint-sqlite 3.1.3。文档调查另含 JavaScript 运行时和 LangSmith 平台，后两者没有参与执行实验。

## 功能分支后续验收

| 原差距 | 本轮进展 | 仍有的边界 |
|---|---|---|
| 服务端单图默认串行 | 顶层图默认 4 个线程。真实 PostgreSQL 用例让慢节点等待快支路审核后的下游动作，该动作成功后慢节点才结束。接管、输入修订与过期审核用例继续通过。 | 单条运行由一个进程推进，嵌套块内部仍串行。 |
| 服务接口与故障重试 | 增加提交预检、结构化错误、Python 客户端和同步业务函数重试。进程在一次临时失败记账后退出，下一进程继续使用剩余尝试。 | 工作进程负责最终编译；支持持久化的边协议范围如前。 |
| 远程分叉 | 主管能查询根位置、预览或执行分叉。两条分叉独立审核，来源运行的结果和历史保持一致。 | 选择无未完成激活的位置；输入覆盖限于可替换的图输入。 |
| 远程查看与资料 | 支持游标跟踪、嵌套作用域历史、根历史状态和授权的项目资料检索。 | 客户端使用轮询；已归档的子图只留溯源信息；搜索覆盖名称和 JSON 文本。 |

本轮在 Linux、Python 3.12、PostgreSQL 16.2 上全量 585 项通过，0 跳过，其中 15 项为 PostgreSQL 集成测试。检查覆盖 v1 迁移后旧运行仍用串行、重试进程中断、预览与可执行分叉、交互式审核的下游先行。pyflakes 与 mypy（75 个源文件）通过。干净环境安装 wheel 后运行 20 个示例，并在另一环境安装 postgres 可选依赖后成功建库。

[接口和操作示例](POSTGRES.md) · [PostgreSQL 集成验收](../tests/test_postgres_collab.py)。以下对照表、命令和测量保留为提交 `324dfab` 的基线记录。

## 基线对照：用户要完成什么

最值得优先解决的问题，是一张在本地成立的协作图，搬到服务器后还能保留多少行为。PostgreSQL 已能让多个人在一条运行上认领和提交任务，也能让其他进程接管。协议覆盖、服务端运行控制、图内并发分别有自己的边界。

| 用户任务 | LangGraph 能做到什么 | Interlacing 的现状与判断 |
|---|---|---|
| 一条支路很慢，让已拿到报价的下游先计算 | 在受测 StateGraph 中，观察者能提前看到快节点的更新，同一步内写入的通道值在下一步供节点读取。[运行时](https://docs.langchain.com/oss/python/langgraph/pregel) | **有具体优势。** 本地 SQLite 持久化引擎在 3 个工作线程下，发送者仍在运行，慢支路也未结束，下游已经读到已提交的报价。这个顺序已实测。PostgreSQL 服务当前以默认的 0 个图内工作线程创建引擎，子图线程上限也是 0。多个服务进程能分担多条运行，单张服务图中的耗时同步节点仍可能挡住其他节点。 |
| 报价从旧值改到新值，找出谁需要返工 | 图可以取历史、修改状态并从检查点派生后续执行。应用也可以记录报价与审核之间的业务依赖。[时间旅行](https://docs.langchain.com/oss/python/langgraph/use-time-travel) | **有具体优势。** 按投递版本记录实际读取者，配置重算的消费轮次进入返工。独立分支与同节点的无关轮次保留结果。真实 PostgreSQL 上已验证旧审核随修订失效、新任务拿到新报价。LangGraph 的对照依据是其公开回放和分叉语义，本轮没有在它上面重建同款审核应用或测开发人时。 |
| 给不同人分派审核，避免重复批准；或在任意业务函数中途临时问人 | 有检查点时，节点可以调用 interrupt()，恢复时以应答作为返回值。实测恢复会重新进入该节点，之前已完成的独立报价节点保持完成。[中断](https://docs.langchain.com/oss/python/langgraph/interrupts) | **各有适用场景。** 多人审核已有任务归属、转派、版本校验和事务续跑。若需要在一个带复杂条件的普通函数中多次暂停，当前受测协作入口是显式 HumanNode，调用者要把这些等待点建成图节点。 |
| 在远程任务里逐段看输出，同时运行会质疑或调用上游的图 | graph.stream/astream 可向调用者提供状态更新、模型 token 和自定义事件。观察输出与节点之间的传输是两种不同能力。[流式输出](https://docs.langchain.com/oss/python/langgraph/streaming) | **两处缺口。** 协作 HTTP 端提供状态查询和根事件分页，调用者需要轮询。引擎普通模式有 stream、call 和带质疑的边；持久化引擎只收没有质疑的 value、artifact 边，也限制激活中途收消息。真实服务接受建单后，工作进程会拒绝带 stream 边的图。此比较没有给 LangGraph 的 token 流赋予跨崩溃精确续传的保证。 |
| 远程主管查看子图卡在哪里，再从旧报价试另一条路线 | 配置检查点后，可读历史、选点回放、修改状态并分叉。[时间旅行](https://docs.langchain.com/oss/python/langgraph/use-time-travel) | **后端与入口有落差。** 本地运行目录已有历史、检查点与可执行分叉测试。PostgreSQL 已存检查点和子作用域，但主管的 history 只返回根事件；实测主管发 fork、checkpoints 均被拒绝。远程主管还缺少对应的暂停、恢复和分叉操作。 |
| 某个业务 API 暂时不可用，只重试这个步骤，永久错误转人工 | 实测节点级 RetryPolicy 按异常类别重试。官方还描述任务结果复用、退避与失败处理。[容错](https://docs.langchain.com/oss/python/langgraph/fault-tolerance) · [函数式任务](https://docs.langchain.com/oss/python/langgraph/functional-api) | **需要更多业务代码。** 已有模型请求退避、运行时限、主管重试失败运行、ctx.once 与外部幂等键。通用业务节点要自行规定哪些异常可重试、尝试次数，以及耗尽后的走向。两套系统与外部服务交互时都要明确副作用边界。 |
| 新运行根据问题找出以前项目里相关的材料 | Store 可按命名空间组织跨线程记忆，配置索引和嵌入模型后可按语义检索。[记忆](https://docs.langchain.com/oss/python/langgraph/add-memory) | **内置检索较弱。** 已有共享和个人资料，按名称、版本读取并固定输入，适合复核来源。目前服务没有按内容搜索资料的操作。确有发现资料的需求时，先支持权限范围内的列举与过滤，再评估接入可替换的检索器。 |
| 现有团队用 TypeScript 编写节点，或用普通代码逐步组织可恢复任务 | 官方提供 JavaScript/TypeScript 运行时，Python 有 Graph API 和可组合的持久化任务。模型和工具适配还可以使用 LangChain 集成。[JavaScript](https://docs.langchain.com/oss/javascript/langgraph/overview) · [函数式任务](https://docs.langchain.com/oss/python/langgraph/functional-api) | **接入成本有差距。** Interlacing 已有 Python 函数节点、JSON 图、CLI、MCP、外部程序和远端节点。TypeScript 客户端能调用 HTTP；要在 TypeScript 中原生编写和调试整张图，当前包没有对应运行时。优先验证跨语言调用的实际需求。 |
| 团队在浏览器里定位问题、比较一批样例在两个版本上的质量 | LangSmith Studio 可看图、调试状态，并与追踪和数据集实验集成。这是配套平台能力。[Studio](https://docs.langchain.com/langsmith/studio) · [评测](https://docs.langchain.com/langsmith/evaluation-types) | **交付体验有差距。** 已有 CLI/MCP、时间线、图导出与自动化测试。协作服务尚缺面向成员的浏览器工作台和成组比较业务结果的实验入口。优先级取决于用户是程序开发者还是实际审核人员。 |
| 判断可承受多少用户、断机后多久恢复 | LangSmith 提供托管部署选项；容量取决于部署方式与负载。[平台](https://docs.langchain.com/langsmith/data-plane) | **缺少同条件证据。** 已实测单机多进程接管与互斥。本轮没有双框架同负载吞吐、跨主机故障转移或数据库与文件联合恢复实测。现有证据支持功能判断，容量排名仍待测量。 |

## 本轮执行记录

| 检查 | 结果与可复核依据 |
|---|---|
| 完整回归 | Linux、CPython 3.12.14、PostgreSQL 16.2、psycopg 3.3.9、MCP 参考客户端 2.3.0。579 项通过，0 跳过，包含 9 项真实 PostgreSQL 集成测试。[服务测试](../tests/test_postgres_collab.py) |
| 代码与交付物 | pyflakes 通过。按项目设置运行 mypy，72 个源文件通过。成功构建 sdist 和 wheel，独立环境只安装 wheel，通过 20 个示例、CLI 和 MCP 冒烟检查，包含 PostgreSQL 建表资源。 |
| 两种调度顺序 | Interlacing SQLite 实测下游在发送者与慢分支结束前消费，进程退出后恢复另有故障注入测试。LangGraph SQLite 实测观察者先收到快节点更新，慢分支完成后下游才开始。用事件协调执行顺序，无吞吐推论。[Interlacing 用例](../tests/test_durable.py) · [LangGraph 对照脚本](../scripts/compare_langgraph.py) |
| 审核与局部返工 | LangGraph 模拟临时故障并成功重试，关闭并重新打开 SQLite 后从审核恢复。Interlacing 另以实际工作进程验证多人认领、重复提交、接管和输入修订；按读取轮次核对无关分支的执行次数。[局部返工](../tests/test_durable_rework.py) · [服务验收](../tests/test_postgres_collab.py) |
| 新增服务边界检查 | 在临时 PostgreSQL 上用主管身份提交两个 HumanNode 通过 stream 边连接的图。start 返回运行编号，worker 将状态记为 failed。报错还沿用了 SQLite 后端名。主管请求 fork 和 checkpoints 时得到不支持的操作。相同服务的正常认领路径在完整回归中通过。 |
| 覆盖边界 | 此轮在单台 Linux 主机执行，未复测 macOS、Windows、真实模型供应商及设备，也未实测 LangSmith 云服务。仓库默认 CI 未配置 PostgreSQL 测试库，会跳过这 9 项；复现本次结果须显式启用数据库。 |

### 复现命令

在仓库根目录执行。第一组先将 INTERLACING_TEST_POSTGRES 设为可建表的**专用临时测试库**连接串，测试会向其中写入数据。下面以 POSIX shell 为例。

```sh
python -m venv /tmp/il-audit
/tmp/il-audit/bin/python -m pip install -e '.[postgres]' pytest mcp pyflakes mypy build
: "${INTERLACING_TEST_POSTGRES:?Set the DSN of a disposable PostgreSQL test database}"
export INTERLACING_TEST_POSTGRES
/tmp/il-audit/bin/python -m pytest -q -rs tests
/tmp/il-audit/bin/python -m pyflakes src tests scripts examples
/tmp/il-audit/bin/python -m mypy src/interlacing
out=$(mktemp -d)
/tmp/il-audit/bin/python -m build --outdir "$out"
python -m venv "$out/clean"
"$out/clean/bin/python" -m pip install --no-deps "$out"/*.whl
repo=$(pwd)
(cd "$out" && "$out/clean/bin/python" "$repo/scripts/package_smoke.py" "$repo/examples/graphs")
```

LangGraph 对照使用独立环境，无需模型密钥。工作流只使用临时 SQLite 文件。

```sh
python -m venv /tmp/lg-audit
/tmp/lg-audit/bin/python -m pip install langgraph==1.2.24 langgraph-checkpoint-sqlite==3.1.3
/tmp/lg-audit/bin/python scripts/compare_langgraph.py
/tmp/il-audit/bin/python -m pytest -q tests/test_durable.py::test_a_committed_delivery_reaches_its_receiver_while_sender_and_sibling_are_running
```

## 基线复核时的优先顺序

以下为候选验收条件。八个既有实施阶段已完成，下列工作尚待立项。

| 顺序 | 目标 | 什么结果算做成 |
|---|---|---|
| 1 | 同一张多人审核图可从本机移到服务端 | 在提交时能指出不支持的节点、边和位置。选择一张含快慢支路的报价图，证明有权的用户能在慢支路结束前收到待办；中途断进程后，已完成计算和已确认的人工答复可恢复。让默认 CI 实际跑 PostgreSQL 用例。 |
| 2 | 让远程主管能看、能解释、能试改 | 只用公开接口，从过期审核追溯到修订源和嵌套节点，按同一历史点生成两个独立方案；核对原运行输出不变、分叉来源可查、其他项目无法读到历史。评估权限和事件订阅后再决定界面形式。 |
| 3 | 用故障和负载证明可部署 | 对一次临时失败、永久失败、重试等待期间重启分别验收尝试次数、最终状态与外部幂等键。两台主机接管同一批运行，演练数据库与内容目录联合恢复。在同一机器、同一输入、相同提交保证下比较两框架首次可用结果时间、P95 等待时间、重算次数与恢复耗时，公开脚本和原始结果。 |
| 按需求 | 资料检索、跨语言作者体验与评测 | 用真实场景决定是否投入。分别验收一次跨项目隔离的检索、一个 TypeScript 客户端完整认领并答复的例子，以及一组修订前后可复跑的业务样本。 |
