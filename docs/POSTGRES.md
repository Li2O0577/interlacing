# PostgreSQL 协作服务

一个项目可以有多位主管和成员。主管创建运行、发布共享资料和分派任务。成员可以认领开放任务、查看分派给自己的请求、提交符合人工节点输出格式的答复，并保存个人资料。多个工作进程使用同一数据库和内容目录，各自推进一条运行。JSONL 和 SQLite 运行目录继续使用原 CLI。

## 启动

使用 Python 3.11 或更高版本，执行 `python -m pip install "interlacing[postgres]==0.17.0"`。本文新增的请求键、浏览器审核和跨层并行需要从当前检出执行 `python -m pip install -e ".[postgres]"`。为服务和工作进程设置 `INTERLACING_POSTGRES_DSN`，连接一个专用 PostgreSQL 数据库。初始化账号需要建表权限。使用文件或 artifact 边时，将 `INTERLACING_CONTENT_DIR` 指向所有进程均能访问的同一个内容目录。客户端使用成员令牌。

```sh
interlacing collab init
interlacing collab project research lead
interlacing collab serve --port 8765
```

第二条命令在终端返回首位主管的令牌。API 监听本机回环地址。跨主机访问时，通过部署方的 HTTPS 反向代理或已认证的隧道连接。另开终端运行 `interlacing collab worker`。可以启动多个进程。每个进程从队列认领一个命令并自动续租。加 `--once` 可处理至多一条命令后退出。工作进程通过 `--allow MODULE` 指定可信 Python 函数，通过 `--models` 启用环境中已配置的模型提供方。项目主管提交的工作流可调用工作进程允许的能力。

## 走通一次人工审核

向 `POST /v1/request` 发送 JSON 请求，使用 `Authorization: Bearer <token>`。以下各行分别作为一次请求。将示例中的占位 ID 和版本替换为前一步的返回值。

```jsonl
{"action":"member","args":{"name":"Alice"}}
{"action":"start","args":{"graph":{"nodes":[{"id":"approve","kind":"human","outputs":[{"name":"answer","schema":"int"}]}],"inputs":{"quote":"approve.request"},"outputs":{"accepted":"approve.answer"}},"inputs":{"quote":"确认报价"},"assignments":{"approve":"Alice"}}}
{"action":"tasks"}
{"action":"submit","args":{"task":"<task UUID>","version":1,"answer":7}}
{"action":"status","args":{"run":"<run UUID>"}}
```

主管用自己的令牌执行前两项和最后一项。Alice 用第一项返回的个人令牌查看任务，然后按任务的当前版本提交。工作进程再次接手后，主管可在 `result.outputs.accepted` 看到 `[7]`。省略 `assignments` 会创建开放任务，成员先调用 `claim`，再用返回的版本提交。处于同一就绪阶段的多个人工节点可同时等待不同成员。嵌套节点的分派名包含外层节点和轮次，例如 `block.1/approve`。

主管可将同一份 `assignments` 传给 `validate` 和 `start`。服务会核对成员和显式声明的节点路径；路径拼错、轮次格式错误或目标是普通程序节点时，错误会指出具体键。对于 `times` 展开等需要编译的结构，预检返回的 `worker_checks` 包含 `assignment_paths`。工作进程在执行首个节点和创建待办之前复核编译后的目标，失败原因可从 `status` 获取。

分派键精确匹配节点和外层轮次。`block.2/approve` 只指定第二轮，其他轮次仍按各自的分派配置处理。服务无法从静态图预测实际会运行几轮。没有分派的人工任务继续开放认领；提交时还不存在、要靠运行中改图才新增的节点不能预先写入分派表。此类新节点按开放任务处理，主管可在出现后用 `assign` 转派。需要从一开始就限定审核人的任务应预先建为人工节点。已有任务的归属不会由升级重写，主管可检查旧待办并按需要转派。

| 操作 | 调用者 | 参数与结果 |
|---|---|---|
| `member` | 主管 | `name`、可选 `role`。返回新成员或主管的令牌 |
| `start`、`status`、`retry` | 主管 | 创建运行并返回 UUID，按 UUID 查状态或重试失败运行 |
| `history`、`audit` | 主管 | 查看运行指定作用域的事件（默认根作用域）或项目操作记录。`history` 支持 `after` 与 `limit` 分页 |
| `tasks`、`claim` | 成员、主管 | 成员看到开放任务和自己的任务，主管可查看整个项目。认领提交任务 UUID 和当前版本 |
| `assign` | 主管 | 使用 `task`、`version`、`member` 分派或转派。转派后旧版本失效 |
| `submit` | 当前认领人 | 传 `task`、`version`、`answer`。相同版本和答复可重试，返回首次提交的版本 |
| `put`、`get` | 成员、主管 | 两者都传 `namespace`（`shared` 或 `personal`）及 `name`。写入传 `data` 或 base64 编码的 `file`，读取传 `version`。共享资料由主管发布 |

`start.resources` 将图输入名映射到资料的命名空间、名称和具体版本。运行使用创建时固定的 JSON 值，文件输入得到包含摘要与大小的引用。成员可以通过 `get` 读取有权限的文件。文件与 artifact 内容使用项目配置的内容存储，PostgreSQL 保存引用、版本和访问范围。个人资料只对其所有者开放。HTTP 每次请求的上限为 16 MiB，包括 base64 编码开销。

## 服务端并行

新运行默认允许整张图同时执行 4 个节点，`start.workers` 可设为 0 至 32，0 表示串行。每条运行继续由一个持有效租约的进程推进。多个进程可以分别接手不同运行。升级已有数据库时，停止旧版服务与工作进程，运行新版 `interlacing collab init`，然后启动新版本。该命令可重复执行，升级前的运行及其分叉保留原有调度设置。

一条 `from → to` 的边指定下游要读取哪个端口。互不依赖的节点可同时运行，依赖节点沿用原图的端口就绪规则。无需增加专门的并行边。快节点的已提交输出可以交给人工审核；当慢支路仍在执行，工作进程也能接收审核答复并推进快支路的下游。如果本次调度已退出，持久化续跑命令会唤起下一次调度。根图和嵌套块共享运行的并发名额。已就绪的作用域按请求顺序获取名额，等待子图或质疑答复的节点会让出名额。块的 `workers` 可进一步限制本块并发度。上限统计正在执行的节点，含协程。协调线程、业务函数自行创建的线程及超时后仍未退出的线程另计。

持久化图支持 `value` 和 `artifact` 边，两者均可配置 `challenges`。主管可先调用 `validate(graph, workers)`，返回已检查项和工作进程仍需检查的项。`start` 执行相同预检，显式声明的 `stream` 和 `call` 边在入队前给出描述中的位置。函数签名、模型配置、编译后的图和运行输入还要由工作进程按其授权目录检查。

## 同一张图上的质疑、答复与工作进程接管

例如报价节点的输出连接审核节点的输入。沿用本地写法即可开启两次质疑额度。

```json
{"from": "quote.out", "to": "review.price", "challenges": 2}
```

审核节点调用 `ctx.reject("price", "报价是否含税？")`，或在协程中等待 `ctx.areject(...)`。解释会把 `explained` 和原文返回给发起者；更正会把 `corrected` 和新值返回给等待这版消息的发起者。其他已经读过旧值的节点继续按各自的更正策略处理。额度耗尽返回 `refused` 和原因。图中已有的人工待办仍可指定成员、认领和提交。若工作流需要人作决定，应显式连接人工节点。

工作进程在已提交质疑后退出，接手者能按运行、嵌套作用域、边及投递编号接续原问题。完整的 `challenge` 事件含消息版本。解释写入 `review_reply`；更正事件的 `answers` 同时记录对应问题和裁决，修订及答复随一次数据库事务提交。制品边沿用原内容目录保存文件。记录不需要新表，部署时先停止旧工作进程，再用新版本同时启动服务与工作进程。旧工作进程会拒绝带质疑的持久化图。

恢复时未完成的业务函数或答复回调可能重新进入。请求外部收费服务、发送消息等副作用仍需使用稳定的业务幂等键。激活中途调用 `ctx.receive()` 的常驻节点仍使用标准模式。[进程接管与多人审核用例](../tests/test_postgres_collab.py) · [断点与内容引用用例](../tests/test_durable_reviews.py)

## Python 客户端与节点重试

客户端只依赖 Python 标准库。示例使用已有运行的编号和主管令牌。HTTP 接口保持 `POST /v1/request`。错误答复含可读的 `error` 和机器可读的 `code`，Python 端的 `ServiceError` 提供 `status` 和 `code`。

```python
from interlacing.collab.client import Client, ServiceError

client = Client("http://127.0.0.1:8765", manager_token)
try:
    current = client.request("inspect", run=run_id)
    for event in client.events(run_id, after=current["event"], follow=True):
        print(event["seq"], event["record"]["kind"])
except ServiceError as error:
    print(error.status, error.code, str(error))
```

对于向业务 API 发请求的同步函数节点，可设 `retry_attempts`（1 至 16，默认 1）。例如 `{"id":"price","kind":"function","function":"shop.quote","retry_attempts":3}`。该函数接受普通输入并返回 JSON 结果。遇到 `ConnectionError` 或 `TimeoutError` 时按指数退避重试，其他异常直接报告。每次完成的尝试都随激活记入日志；中断接管后沿用尝试次数和退避截止时间。外部有副作用的请求应携带业务幂等键，以覆盖外部服务响应到本地记账之间的崩溃窗口。带 `ctx` 的函数和异步函数仍自行管理重试。

可用 `"retry_on": ["timeout"]` 只自动重试超时，也可选 `"connection"` 或两者。主管对失败运行发起 `retry` 后，已经耗尽预算的激活可以重新调用业务函数。数据库命令编号区分新授权与进程接管。已经成功并记账的调用继续复用原结果。创建运行时可传 `request_key`（1 至 256 个字符）。同项目同主管带相同键和参数重发会得到首次运行编号。参数冲突返回 HTTP 409。有意新开一次运行时更换键。省略键沿用每次创建的行为。

## 实时观察根运行

主管使用 `Client.stream_progress` 获取运行状态、节点开始（`bind`）、提交（`act`）、投递、更正和自定义阶段。同步或异步业务节点可调用 `ctx.progress("已校验报价，正在生成复核摘要")`。阶段文本向本项目主管可见。激活在崩溃后重新进入时，可以再次报告同一阶段。

```python
cursor = 0
for frame in client.stream_progress(run_id, after=cursor):
    if frame["type"] == "event":
        print(frame["seq"], frame["kind"], frame.get("node"), frame.get("message"))
        cursor = frame["seq"]  # 在显示或处理成功后保存，重连时传回
    else:
        print(frame["state"])
```

`GET /v1/progress?run=UUID&after=N` 使用 Bearer 认证，按行传输 JSON。事件序号与根历史一致。状态帧携带当前状态及读到的位置，空闲时约每五秒发送一次。终态的最后一页发送完毕后关闭连接。HTTP 服务为每个订阅独立读取已提交事件，追上后每 250 毫秒检查一次。观察者断开不会取消执行。数据库历史分页仍可用于查看嵌套激活。此入口提供运行进度，模型逐 token 显示属于另一项能力。

## 远程检查、执行分叉与资料查找

| 操作 | 输入及结果 |
|---|---|
| `checkpoints`、`inspect`（主管） | 按 `run` 列出根事件末尾与保留的检查点。`inspect(run, at)` 返回该事件的输出、待完成的激活、待返工项及能否分叉。`current_status` 是查询当下的运行状态。 |
| `fork`（主管） | 提供 `run`、根事件序号 `at`，可选 `inputs` 替换尚未消费的图输入，或用 `output` 指定要修订的节点消息。默认创建可查看的预览，显式传 `execute: true` 会在独立运行中执行后续节点。结果含新运行 UUID 和来源位置。 |
| `history`（主管） | 缺省读取根历史，可传 `scope: [["block",1],["child",1]]` 选择嵌套激活。运行中的作用域返回事件；已清理的子日志返回留存的溯源元数据，并标记 `archived: true`。两者都按 `seq` 分页。客户端的 `events` 可用游标继续轮询。 |
| `resources`（项目成员、主管） | `namespace` 选 shared 或 personal。`prefix` 按名称前缀过滤，`query` 对名称与 JSON 文本执行不区分大小写的子串匹配。文件只匹配名称。结果含名称、版本和种类，下一页将 `next` 传为 `after`。使用 `get` 取回选定版本。 |

分叉事务从选定事件重建已提交状态。它保留来源、已完成的工作和当时的输入版本，新任务使用独立编号。未完成的激活须先结束，或选择更早的位置。旧版运行若缺少已编译的端口描述，仍可原样分叉；覆盖输入时需要能够还原端口类型。可执行分叉可能产生新的外部调用。

```python
# 把 run_id 的初始报价换成另一份，先查看预览的来源。
preview = client.request("fork", run=run_id, at=1, inputs={"quote": "新版报价"})
print(client.request("inspect", run=preview["run"])["origin"])
matches = client.request("resources", namespace="shared", query="报价", limit=20)
for item in matches["items"]:
    print(item["name"], item["version"])
```

### 从已审核的中间结论建立新分支

从 `history` 取目标 `deliver` 的 `node` 和 `token`。首次投递是版本 1，后续 `correct` 记录给出新版本。选择该消息已提交且 `inspect(at)` 返回 `forkable` 的位置。以下假设报价节点已发出一条名为 `price#1` 的消息。

```python
edit = {"node": "quote", "token": "price#1", "version": 1,
        "value": 120, "reason": "供应商补发了含税报价单"}
branch = client.request("fork", run=run_id, at=position, output=edit, execute=True)
```

新运行记录主管身份和理由，按实际读取关系及原图的更正策略投递。默认要求返工的下游若已审核，会在分叉中收到新任务。原运行的签字和历史继续可查。省略 `execute` 可先预览，预览不会调度节点。本地持久化 SQLite 入口为 `Run.fork(..., output=edit)` 或 `interlacing fork --output '<JSON>'`，本地编辑者记为 `local`。首版支持静态根图上经普通前向 value 边传递的 JSON，以及直接图输出。要求稳定位置、匹配的消息版本、端口类型和修改理由；自定义输出校验、回边和 artifact 投递需要另外支持。部署时服务和工作进程使用同一版本。

## 提交与接管

运行租约使用数据库时间和递增代次。工作进程每次写入运行日志、检查点、任务或完成状态时，事务都会锁定运行并核验当前代次。进程退出后，其未完成命令可在租约到期后被另一个进程认领。普通节点仍以图内提交记录为恢复依据，外部写入沿用节点自身的幂等机制。

人工答复、续跑命令和审计记录在同一事务提交。任务绑定运行、节点位置、激活轮次和版本。当消息修订使人工节点或包含它的子图需要重算，相关待办会在消息提交事务内失效。旧任务拒收答复，新任务展示修订后的请求。校验失败的提交保留原任务状态。共享目录提供文件字节，数据库备份应搭配该目录的备份。操作系统级执行隔离以及跨主机内容目录的部署由宿主管理。

本分支已实测停写后同时备份 PostgreSQL 与内容目录，在独立数据库恢复后读取原文件并完成原待办。持续写入时取得两者一致快照、跨主机恢复和恢复耗时仍需验证。[恢复演练记录](BRANCH_REVIEW.md)

使用独立的测试数据库设置 `INTERLACING_TEST_POSTGRES` 后，运行 `python -m pytest -q tests/test_postgres_collab.py`，可验证双进程执行、租约接管、并发认领、提交回滚、任务和资料访问范围。

## 在浏览器里审核一次数学建模返工

升级后先运行 `interlacing collab init`。使用测试项目，主管按前面的步骤创建成员 Alice 和 Bob。
从仓库根目录进入 `examples/review`，在该目录启动 `interlacing collab worker --allow model_review`。
另一个终端设置主管令牌环境变量 `MANAGER_TOKEN`，在相同目录执行 `python model_review.py`，记下返回的运行 UUID。

1. Alice 和 Bob 分别打开服务的 `/review`，输入各自的成员令牌。Alice 为 q1 提交整数 9，为独立的 q3 提交 5。Bob 刷新后读取 q1 的结果，在 q2 提交预测值 18。
2. 主管确认运行已完成后执行 `python model_review.py --revise 原运行UUID --value 12`，创建独立分叉。
3. Bob 刷新并选择新运行的 q2，比较原输入 9 和当前输入 12，查看修改人和理由，提交 24。Alice 的 q3 结果沿用。新报告为 24 和 5，原报告仍为 18 和 5。
4. 主管用自己的令牌打开分叉任务，刷新后可查看已启动、已修订和当前沿用结果的根节点。普通成员仅能读取可见任务的修订信息；历史输入只向有权读取对应旧任务的人展示。页面使用内存中的令牌，刷新页面需重新登录。

任务详情也可用 `review(task=UUID)` 获取。对照信息来自任务绑定的具体投递和修订事件，嵌套任务同时记录它的外层输入依赖。
