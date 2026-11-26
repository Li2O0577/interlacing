# PostgreSQL 协作服务

一个项目可以有多位主管和成员。主管创建运行、发布共享资料和分派任务。成员可以认领开放任务、查看分派给自己的请求、提交符合人工节点输出格式的答复，并保存个人资料。多个工作进程使用同一数据库和内容目录，各自推进一条运行。JSONL 和 SQLite 运行目录继续使用原 CLI。

## 启动

安装 `pip install 'interlacing[postgres]'`。为服务和工作进程设置 `INTERLACING_POSTGRES_DSN`，连接一个专用 PostgreSQL 数据库。初始化账号需要建表权限。使用文件或 artifact 边时，将 `INTERLACING_CONTENT_DIR` 指向所有进程均能访问的同一个内容目录。客户端使用成员令牌。

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

| 操作 | 调用者 | 参数与结果 |
|---|---|---|
| `member` | 主管 | `name`、可选 `role`。返回新成员或主管的令牌 |
| `start`、`status`、`retry` | 主管 | 创建运行并返回 UUID，按 UUID 查状态或重试失败运行 |
| `history`、`audit` | 主管 | 查看根运行事件或项目操作记录。`history` 支持 `after` 与 `limit` 分页 |
| `tasks`、`claim` | 成员、主管 | 成员看到开放任务和自己的任务，主管可查看整个项目。认领提交任务 UUID 和当前版本 |
| `assign` | 主管 | 使用 `task`、`version`、`member` 分派或转派。转派后旧版本失效 |
| `submit` | 当前认领人 | 传 `task`、`version`、`answer`。相同版本和答复可重试，返回首次提交的版本 |
| `put`、`get` | 成员、主管 | 两者都传 `namespace`（`shared` 或 `personal`）及 `name`。写入传 `data` 或 base64 编码的 `file`，读取传 `version`。共享资料由主管发布 |

`start.resources` 将图输入名映射到资料的命名空间、名称和具体版本。运行使用创建时固定的 JSON 值，文件输入得到包含摘要与大小的引用。成员可以通过 `get` 读取有权限的文件。文件与 artifact 内容使用项目配置的内容存储，PostgreSQL 保存引用、版本和访问范围。个人资料只对其所有者开放。HTTP 每次请求的上限为 16 MiB，包括 base64 编码开销。

## 提交与接管

运行租约使用数据库时间和递增代次。工作进程每次写入运行日志、检查点、任务或完成状态时，事务都会锁定运行并核验当前代次。进程退出后，其未完成命令可在租约到期后被另一个进程认领。普通节点仍以图内提交记录为恢复依据，外部写入沿用节点自身的幂等机制。

人工答复、续跑命令和审计记录在同一事务提交。任务绑定运行、节点位置、激活轮次和版本。当消息修订使人工节点或包含它的子图需要重算，相关待办会在消息提交事务内失效。旧任务拒收答复，新任务展示修订后的请求。校验失败的提交保留原任务状态。共享目录提供文件字节，数据库备份应搭配该目录的备份。操作系统级执行隔离以及跨主机内容目录的部署由宿主管理。

使用独立的测试数据库设置 `INTERLACING_TEST_POSTGRES` 后，运行 `python -m pytest -q tests/test_postgres_collab.py`，可验证双进程执行、租约接管、并发认领、提交回滚、任务和资料访问范围。
