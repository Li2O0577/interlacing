# 展示与部署

适用版本为已包含 PR #52 的仓库检出。以下命令适用于 Linux、Python 3.11+ 和 PostgreSQL 16。服务、工作进程和节点模块使用同一份代码。以下示例目录为 `/opt/interlacing/app`，虚拟环境为 `/opt/interlacing/venv`。

## 先跑完整演示

在仓库根目录安装项目。本手册在 PostgreSQL 16 服务端和 16 版客户端组合上验收。先核对 `pg_dump --version` 和 `pg_restore --version`。如果 Linux 安装了多个大版本，选择 16 版客户端，例如执行 `export PATH="/usr/lib/postgresql/16/bin:$PATH"`。新版工具生成的恢复语句可能被旧服务器拒绝。演练还需要 GNU `sha256sum`。

```sh
python3 -m venv /opt/interlacing/venv
. /opt/interlacing/venv/bin/activate
python -m pip install '.[postgres]'
# libpq 通过 PGHOST、PGPORT、PGUSER 和 .pgpass 连接测试实例。
# 此处使用可以建临时库的演练账号。
python scripts/rehearse_deployment.py
```

脚本创建两个随机命名的临时数据库，并在退出时清理。它启动真实 HTTP 服务和 worker，访问审核页，然后用 Alice 和 Bob 的身份通过 API 走完与页面相同的流程。看到 `original 18/5, revised 24/5` 即验证了第一问从 9 修订到 12，只让依赖它的第二问返工。随后输出容量、分叉及备份恢复数据。现场手动点击的顺序见[多人建模审核](POSTGRES.md#在浏览器里审核一次数学建模返工)。

如需展示调度时序，在独立虚拟环境运行以下对照。脚本输出观察者收到更新、慢节点结束、下游开始消费的相对时间，以及观察者先后顺序的次数。Interlacing 的对应回归另列在[对照说明](LANGGRAPH_COMPARISON.md#复现命令)。

```sh
python -m pip install langgraph==1.2.24 langgraph-checkpoint-sqlite==3.1.3
python scripts/compare_langgraph.py --repeat 20
```

## 单机常驻运行

准备一个专用数据库、一个服务系统账号 `interlacing` 和它可写的内容目录。安装时的数据库账号需要建表权限。服务与 worker 必须看到相同的内容字节。以下示例把连接配置存入服务账号可读的 `/etc/interlacing/service.env`。数据库密码可由该账号权限为 0600 的 `.pgpass` 提供。

```ini
PGHOST=127.0.0.1
PGPORT=5432
PGUSER=interlacing
PGDATABASE=interlacing
INTERLACING_POSTGRES_DSN=dbname=interlacing
INTERLACING_CONTENT_DIR=/var/lib/interlacing/content
```

以服务账号加载上述环境后执行。首次建项目会输出主管令牌，应交给演示主管。升级时先备份，停止该版本的全部服务和 worker，安装新版本，执行可重复的 `collab init`，再启动进程。回滚需要保留旧代码、配套数据库备份和内容目录。

```sh
set -a
. /etc/interlacing/service.env
set +a
mkdir -p "$INTERLACING_CONTENT_DIR"
/opt/interlacing/venv/bin/interlacing collab init
/opt/interlacing/venv/bin/interlacing collab project research lead
```

以下为 systemd 单元模板 `/etc/systemd/system/interlacing@.service`。使用两个实例分别启动 API 和 worker。

```ini
[Unit]
Description=Interlacing collaboration %i
After=network-online.target
Wants=network-online.target

[Service]
User=interlacing
WorkingDirectory=/opt/interlacing/app
EnvironmentFile=/etc/interlacing/service.env
EnvironmentFile=-/etc/interlacing/%i.env
ExecStart=/opt/interlacing/venv/bin/interlacing collab %i $INTERLACING_ARGS
Restart=on-failure
RestartSec=3
UMask=0077

[Install]
WantedBy=multi-user.target
```

演示 worker 使用以下 `/etc/interlacing/worker.env`。部署自己的图时，将模块和目录换成对应的只读业务代码。跨主机时须让各机器上的服务账号都能读写共享目录，包括实际创建的文件权限。

```ini
PYTHONPATH=/opt/interlacing/app/examples/review
INTERLACING_ARGS=--allow model_review
```

```sh
systemctl daemon-reload
systemctl enable --now interlacing@serve interlacing@worker
systemctl is-active interlacing@serve interlacing@worker
curl -fsS http://127.0.0.1:8765/review >/dev/null
```

页面探针只检查 HTTP。完整验收还要让成员提交答复，并由主管查看 `status` 中的最终报告。外部成员通过 HTTPS 反向代理或已认证隧道访问回环地址。主管和普通成员分别使用各自令牌。[HTTP 操作与权限](POSTGRES.md)

## 在线备份和恢复

在备份主机加载同一组 `PG*` 参数和 `INTERLACING_CONTENT_DIR`，确认 `PGDATABASE` 指向服务数据库。`pg_dump` 和内容清单使用同一个 PostgreSQL 快照。脚本按该快照的 `il_files` 引用复制文件，逐个核对长度和 SHA-256，将数据库归档和文件校验和一起发布。备份期间可继续追加内容，须保留快照引用的文件直至复制完成。

```sh
# BACKUP_ROOT 指向已挂载的备份存储，运行账号可写。
backup="$BACKUP_ROOT/interlacing-$(date -u +%Y%m%dT%H%M%SZ)"
python scripts/backup_collab.py "$backup"
(cd "$backup" && sha256sum --check --quiet SHA256SUMS)
```

在**新数据库和新目录**恢复。以下示例要求数据库名与内容目录均未使用，并在任何检查失败时停止。备份保存数据库及受管理的内容文件。服务账号、数据库角色、节点代码、模型凭据和代理配置需在目标环境另行准备。

```sh
(
  set -eu
  cd "$backup"
  sha256sum --check --quiet SHA256SUMS
  test ! -e /var/lib/interlacing/restored-content
  createdb interlacing_restore
  pg_restore --exit-on-error --single-transaction --no-owner -d interlacing_restore database.dump
  cp -a content /var/lib/interlacing/restored-content
)
```

恢复验收时让隔离的服务与 worker 同时指向恢复库和新目录，禁用外部业务副作用或使用演练替身。先读一个已知版本的文件，再接着完成原待办、检查原运行的最终输出。实际切换时先停止旧端写入，确认恢复点之后的业务处理方式，再切换入口。从备份恢复会回到该快照对应的位置。

## 两台主机的上线验收

在隔离环境准备一套空测试数据库和一个两台主机都挂载的测试内容目录。两个临时 worker 应独占这套队列。A、B 使用相同版本、相同数据库和共享目录，并各自设置 `INTERLACING_POSTGRES_DSN` 和 `INTERLACING_CONTENT_DIR`。

1. 在 A 运行 `python scripts/rehearse_takeover.py crash`。记下输出的运行 UUID。进程在输入绑定已提交后以状态码 43 退出。
2. 在 B 运行 `python scripts/rehearse_takeover.py recover <运行UUID>`。B 等待 3 秒测试租约自然到期，检查第 2 代成功写完原运行，历史中只有一次绑定和一次完成，并校验从 A 发布的文件字节。
3. 保存两个主机名、运行 UUID、耗时与日志。演练的同机版本已通过。目标网络和共享卷上的双机结果需在部署现场取得。正式部署还应按真实负载及数据库规模重测容量、备份窗口、可接受的数据损失和恢复时间。

## 本轮实测

2026-11-27，Linux 单机，CPython 3.12.14，PostgreSQL 16.2，本地磁盘和回环网络。应用基线为 `f88debd`。每批先提交 32 条单节点传值图，再启动进程消费积压。首个结果和吞吐从第一笔提交前开始计时。P95 取每条运行从开始提交至认领返回的时长，排序后取第 31 个值。以下为一轮样本。

| 工作进程 | 首个结果 | P95 提交至认领 | 整批吞吐 |
|---|---:|---:|---:|
| 1 | 173 ms | 1,242 ms | 22.18 条/秒 |
| 2 | 170 ms | 682 ms | 37.68 条/秒 |

固定 256 字节输入，在已完成串行图的最后一个事件处创建预览分叉，各测三次。末列为一个子运行全部事件的 JSON 文本字节数，物理存储还包含行、索引、检查点与 WAL。

| 源节点 | 源事件 | 分叉三次耗时（ms） | 子运行事件字节 |
|---:|---:|---|---:|
| 1 | 5 | 5.36 / 5.19 / 4.99 | 1,471 |
| 16 | 50 | 6.55 / 5.88 / 6.30 | 13,501 |
| 64 | 194 | 10.52 / 9.29 / 10.29 | 52,362 |
| 512 | 1,538 | 50.12 / 50.86 / 51.22 | 419,451 |

- 在线备份用时 146 ms，其间成功提交了 13 次文件更新。源库最后达到第 15 版，恢复库精确读取了快照内的第 1 版；备份结束后写入的标记在恢复库中不存在。校验归档、恢复数据库和内容、核对文件并接着完成原待办，共用 246 ms。大库、网络存储和真实业务的恢复目标需要另测。
- 两个本地进程执行接管脚本，在 3 秒租约下，从启动接管命令到核对完成用时 2.77 秒。还需在不同主机上跑同一组命令并记录结果。
