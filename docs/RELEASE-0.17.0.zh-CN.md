Interlacing 0.17.0 将持久化工作流和多人协作纳入正式发行版。已有本地工作流可以继续使用默认的 JSONL 日志。需要检查历史、从中间结论重新执行，或将人工审批分派给项目成员时，可以按需启用 SQLite 或 PostgreSQL。

### 本次更新

- **检查历史与分叉运行**。持久化 SQLite 运行可以检查历史事件。在静态根图的稳定位置，可以创建独立运行，修订一条已提交的 JSON 输出，并按图中原有的更正规则执行下游节点。原运行的结果和审批记录保留，可用于比较两次结论。
- **多人任务与审批**。PostgreSQL 服务支持项目成员、个人与共享资料，以及人工任务的分派、认领和提交。任务关联输入版本。上游更正触发返工时，旧审批任务失效，需要重新审核的人员会收到新任务。
- **并行执行与故障接管**。独立的根图分支可由多个工作进程并行执行。进程中断后，其他进程可在租约到期后接管。同步函数可针对连接错误和超时设置重试次数，接管时沿用剩余次数。
- **实时进度**。节点可用 `ctx.progress()` 报告业务阶段。客户端通过 `Client.stream_progress()` 订阅根运行事件，断线后可从上次记录的事件序号继续读取。

### 安装与升级

需要 Python 3.11 或更高版本。本地运行（包括 SQLite）使用核心包：

```sh
python -m pip install "interlacing==0.17.0"
```

部署多人协作服务时，在服务端和工作进程上安装相同版本，并按指南配置 PostgreSQL。使用文件边时，各进程还需要访问同一内容目录。

```sh
python -m pip install "interlacing[postgres]==0.17.0"
```

[中文使用手册](https://github.com/Li2O0577/interlacing/blob/v0.17.0/docs/HANDBOOK.zh-CN.md) · [PostgreSQL 部署与迁移](https://github.com/Li2O0577/interlacing/blob/v0.17.0/docs/POSTGRES.md) · [与 v0.16.2 比较](https://github.com/Li2O0577/interlacing/compare/v0.16.2...v0.17.0)
