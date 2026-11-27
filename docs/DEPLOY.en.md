# Demonstration and deployment

**English** · [中文](DEPLOY.md)

Use a checkout that includes the rehearsal scripts from PR #53 and the collaboration features from PR #52. These commands assume Linux, Python 3.11 or later and PostgreSQL 16. Install the same revision of the service, workers and node modules. The examples use `/opt/interlacing/app` for the checkout and `/opt/interlacing/venv` for its virtual environment.

## Run the full demonstration

Start at the repository root. The rehearsal was verified with PostgreSQL 16 server **and client tools**. Check `pg_dump --version` and `pg_restore --version`. On Linux installations with several versions, select the version 16 tools, for example with `export PATH="/usr/lib/postgresql/16/bin:$PATH"`. A newer client can generate restore statements an older server rejects. The rehearsal also needs GNU `sha256sum`.

```sh
python3 -m venv /opt/interlacing/venv
. /opt/interlacing/venv/bin/activate
python -m pip install '.[postgres]'
# libpq uses PGHOST, PGPORT, PGUSER and .pgpass to reach the test instance.
# Use a rehearsal account that can create temporary databases.
python scripts/rehearse_deployment.py
```

The script creates two temporary databases with randomized names and cleans them up on exit. It starts an HTTP service and worker, opens the review endpoint, and uses the API to carry out the same steps that Alice and Bob perform in the browser. Look for `original 18/5, revised 24/5`. Revising the first answer from 9 to 12 sent its dependent question back for review and preserved the independent answer. The script then prints capacity, fork and backup/restore measurements. To run the browser demonstration yourself, follow the [multi-user modeling review](POSTGRES.en.md#review-a-revised-modeling-result-in-the-browser).

For a scheduling demonstration, run the following in a separate virtual environment. The probe measures three times independently: when the observer sees an update, when the slow node finishes and when its downstream consumer starts. It reports how often the observer's update came before or after the slow node finished. The corresponding Interlacing regression is [the committed-delivery test](../tests/test_durable.py). [Comparison commands and historical measurements (Chinese)](LANGGRAPH_COMPARISON.md#复现命令)

```sh
python -m pip install langgraph==1.2.24 langgraph-checkpoint-sqlite==3.1.3
python scripts/compare_langgraph.py --repeat 20
```

## Run a persistent single-host service

Prepare a dedicated database and an OS service account named `interlacing` with a writable content directory. The database account used for initialization needs permission to create tables. The service and workers must see the same content bytes. Store the connection settings in `/etc/interlacing/service.env`, readable by the service account. The account can supply its database password via a mode-0600 `.pgpass` file.

```ini
PGHOST=127.0.0.1
PGPORT=5432
PGUSER=interlacing
PGDATABASE=interlacing
INTERLACING_POSTGRES_DSN=dbname=interlacing
INTERLACING_CONTENT_DIR=/var/lib/interlacing/content
```

As the service account, load that environment and initialize the service. The project creation command prints the first manager token. Give it to the person managing the demo. For an upgrade, back up first, stop all service and worker processes for that installation, install the new revision, run the repeatable `collab init`, then restart. Keep the previous code, matching database backup and content directory for rollback.

```sh
set -a
. /etc/interlacing/service.env
set +a
mkdir -p "$INTERLACING_CONTENT_DIR"
/opt/interlacing/venv/bin/interlacing collab init
/opt/interlacing/venv/bin/interlacing collab project research lead
```

Install this systemd template as `/etc/systemd/system/interlacing@.service`. Start separate instances for the API and worker.

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

For the demo worker, put the following in `/etc/interlacing/worker.env`. For your own graph, substitute its module and read-only application code directory. Across hosts, make sure each service account can read and write the shared directory, including files created by the other account.

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

The page probe checks the HTTP endpoint. To verify the whole deployment, have a member submit an answer and a manager inspect the final report using `status`. Remote members reach the loopback listener through an HTTPS reverse proxy or an authenticated tunnel. Managers and members each use their own tokens. [HTTP operations and permissions](POSTGRES.en.md)

## Back up and restore during live writes

On the backup host, load the same `PG*` connection settings and `INTERLACING_CONTENT_DIR`, and confirm that `PGDATABASE` names the service database. The database dump and list of content references use a single PostgreSQL snapshot. The script copies every object referenced by `il_files` in that snapshot, verifying its length and SHA-256 digest. It publishes checksums alongside the archive. Writers may append content during backup. Retain files referenced by the snapshot until copying finishes.

```sh
# Set BACKUP_ROOT to a mounted backup volume writable by this account.
backup="$BACKUP_ROOT/interlacing-$(date -u +%Y%m%dT%H%M%SZ)"
python scripts/backup_collab.py "$backup"
(cd "$backup" && sha256sum --check --quiet SHA256SUMS)
```

Restore to a **new database and a new content directory**. The example expects both destination names to be unused and stops on a failed check. The backup contains the database and managed content. Provision the service account, database roles, node code, model credentials and proxy settings on the target separately.

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

Point an isolated service and worker at the restored database and content directory. Disable external business effects or use rehearsal substitutes. Read a known file version, finish an existing pending task and check the final output of the original run. For cutover, stop writes to the old deployment and decide how to reconcile operations after the recovery point before switching the endpoint. The restored state corresponds to the backup snapshot.

## Accept a two-host deployment

Use an empty test database and a test content directory mounted on both hosts. Reserve the queue for the two rehearsal workers. Hosts A and B must use the same code, database and shared directory. Set `INTERLACING_POSTGRES_DSN` and `INTERLACING_CONTENT_DIR` on each host.

1. On A, run `python scripts/rehearse_takeover.py crash` and record the printed run UUID. Exit code 43 is expected after the input binding commits.
2. On B, run `python scripts/rehearse_takeover.py recover RUN_UUID`, substituting the recorded UUID. B waits for the three-second test lease to expire. It checks that the second generation completed the original run, that the history contains one binding and one completion, and that B can read the bytes published on A.
3. Save both hostnames, the run UUID, elapsed time and logs. This probe passed between two processes on one host. Run it on the actual hosts to verify their network and shared volume. Measure capacity, backup window, acceptable data loss and recovery time with the expected database size and load.

## Recorded measurements

Single Linux host, CPython 3.12.14, PostgreSQL 16.2, local disk and loopback networking, 2026-11-27. Application baseline: `f88debd`. Each batch queued 32 single-node value-passing graphs before starting workers. First-result latency and throughput use the time immediately before the first submission as their origin. Submit-to-claim latency starts before each submission and ends when the worker's claim returns. The reported P95 is the 31st of 32 sorted values. These figures come from one batch per configuration.

| Workers | First result | P95 submit to claim | Batch throughput |
|---|---:|---:|---:|
| 1 | 173 ms | 1,242 ms | 22.18 runs/s |
| 2 | 170 ms | 682 ms | 37.68 runs/s |

With a fixed 256-byte input, each trial created a preview fork at the last event of a completed serial graph. The final column counts the child run's event JSON text. Physical storage also includes rows, indexes, checkpoints and WAL.

| Source nodes | Source events | Three fork times (ms) | Child event bytes |
|---:|---:|---|---:|
| 1 | 5 | 5.36 / 5.19 / 4.99 | 1,471 |
| 16 | 50 | 6.55 / 5.88 / 6.30 | 13,501 |
| 64 | 194 | 10.52 / 9.29 / 10.29 | 52,362 |
| 512 | 1,538 | 50.12 / 50.86 / 51.22 | 419,451 |

- Alice and Bob submitted four answers across two separate Chromium sessions. Bob saw both the original input 9 and revised input 12 on the reopened task. The reports were 18/5 and 24/5.
- The online backup took 146 ms, overlapping 13 committed file updates. The source reached version 15. The restored database contained version 1 from the snapshot, and the marker written after backup was absent. Verifying the archive, restoring the database and files, checking the content and completing the pending review took 246 ms. Larger databases and network storage need measurements under the intended workload.
- With a three-second lease, recovery and verification between two local processes took 2.77 seconds from the start of the recovery command. Collect separate results on two hosts.
