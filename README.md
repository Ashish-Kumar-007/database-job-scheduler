# Database-Backed Job Scheduler

A multi-tenant job scheduler that uses PostgreSQL as both its durable job store
and its coordination mechanism. Independent consumer processes claim due jobs
transactionally and execute them with bounded worker-thread pools.

> **Status:** Hiring-assignment implementation with a reproducible local demo
> and PostgreSQL integration tests. It is not a managed service or a guarantee
> of exactly-once execution for arbitrary external side effects.

## Contents

- [Capabilities and guarantees](#capabilities-and-guarantees)
- [Requirements](#requirements)
- [Quick start](#quick-start)
- [Submit a job](#submit-a-job)
- [Run consumers](#run-consumers)
- [Run the end-to-end demo](#run-the-end-to-end-demo)
- [Configuration](#configuration)
- [Tests](#tests)
- [Architecture and limitations](#architecture-and-limitations)

## Capabilities and guarantees

- PostgreSQL 16+, SQLAlchemy 2.x, psycopg 3, and Alembic migrations.
- Atomic claims use row locks and `FOR UPDATE SKIP LOCKED`; claim transactions
  commit before handler execution.
- Database-time scheduling checks, lease renewal and recovery, stale-token
  fencing, bounded retries, and terminal failure states.
- Persistent, unweighted round-robin dispatch with per-organization concurrency
  limits and bounded per-process worker pools.
- Structured lifecycle logs and consumer summaries with outcome counts,
  throughput, and scheduling-delay statistics.
- A durable sample idempotency operation, protected by a unique job identifier.

The scheduler provides recoverable, at-least-once processing and at most one
valid active claim per job. A lease expiring does not prove its worker stopped.
Claim tokens prevent stale workers from changing current database state; they
cannot undo an external side effect. A crash after an external side effect but
before completion is persisted can cause that side effect to run again. For
external integrations, use a destination-supported idempotency key such as the
stable job UUID.

## Requirements

- Python 3.12 or later
- Docker Engine with Docker Compose
- PostgreSQL 16 or later (the Compose setup uses PostgreSQL 16)

The commands below use PowerShell on Windows. Adapt virtual-environment
activation and environment-variable syntax for other shells.

## Quick start

Run the demo and tests only against a **dedicated, isolated development/test
database**. Never point demo or test commands at production or a shared
application database. The automated demo checks that the `jobs` table is empty
before starting, but that check does not protect against other processes
writing to the same database concurrently.

### 1. Configure and start PostgreSQL

```powershell
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
docker compose up -d postgres
docker compose ps
```

The sample credentials in `.env.example` are for local development only. If you
change `POSTGRES_HOST_PORT`, set the same port in `DATABASE_URL`. The Compose
volume preserves data between restarts; `docker compose down --volumes` deletes
that local database volume.

### 2. Install the application

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[dev]"
```

Load the values from `.env` into the current PowerShell session. This does not
print the database URL:

```powershell
Get-Content .env | Where-Object { $_ -match '^[A-Z_]+=' } | ForEach-Object {
    $name, $value = $_ -split '=', 2
    Set-Item -Path "Env:$name" -Value $value
}
```

### 3. Check the database and apply migrations

```powershell
python -m app
python -m alembic upgrade head
```

The health check executes `SELECT 1`. Migrations—not application startup—own
schema creation and upgrades. To inspect the current migration revision:

```powershell
python -m alembic current
```

## Submit a job

The application exposes services without requiring a web framework. For
example, submit a job due in five minutes:

```python
from datetime import UTC, datetime, timedelta

from app.config import Settings
from app.db.engine import create_engine
from app.db.session import create_session_factory
from app.repositories.jobs import JobRepository
from app.repositories.organizations import OrganizationRepository
from app.services.jobs import JobService
from app.services.organizations import OrganizationService

engine = create_engine(Settings.from_env())
try:
    sessions = create_session_factory(engine)
    organization = OrganizationService(
        OrganizationRepository(sessions)
    ).create("example-organization", max_concurrent_jobs=2)
    job = JobService(JobRepository(sessions)).submit(
        organization_id=organization.id,
        job_type="sample.success",
        payload={"message": "hello"},
        scheduled_at=datetime.now(UTC) + timedelta(minutes=5),
        max_attempts=3,
    )
    print(f"Submitted job {job.id}")
finally:
    engine.dispose()
```

`payload` must be JSON-compatible. Timestamps must be timezone-aware.
`attempts` counts committed claims, including claims recovered after a worker
crash. A completed or terminally failed job is not claimable again.

## Run consumers

Start one consumer in each terminal after loading `.env` into each session:

```powershell
python -m app.consumers
```

Consumers have independent database engines and process identities. To start
three local consumers from one PowerShell session:

```powershell
1..3 | ForEach-Object { Start-Process python -ArgumentList "-m app.consumers" }
```

Press Ctrl+C in a terminal to stop a consumer gracefully and drain in-flight
handlers. A bounded run for smoke testing is:

```powershell
python -m app.consumers --max-completed 5
```

The CLI registers `sample.success`, `sample.slow`, `sample.failure`, and
`sample.idempotent`. Database connection errors use capped exponential
backoff with jitter. Logs include job and tenant identifiers, attempts,
outcomes, and consumer summaries without including payloads.

## Run the end-to-end demo

Use a dedicated PostgreSQL database with no existing jobs and no other active
consumers or producers. Load its settings as described in the quick start, then
run:

```powershell
python -m alembic upgrade head
python -m scripts.demo run
```

The demo creates 60 workload jobs across three organizations and one isolated
crash-recovery job in a fourth organization. It includes fast successes, slow
jobs, retryable failures, idempotent operations, and future-scheduled jobs. A
child process is terminated after writing the crash job's durable idempotent
side effect; the demo waits for its lease to expire, then launches three
consumer processes.

The command waits for every demo job to reach `completed` or `failed`, verifies
that future jobs were not started early, and reports per-tenant/per-handler
counts, retries, recovered jobs, scheduling delay, throughput, and reconciliation
of submitted jobs. The demo data is retained for inspection. The command prints
a run ID; use it to report or remove only that run:

```powershell
python -m scripts.demo report --run-id <run-id>
python -m scripts.demo cleanup --run-id <run-id> --confirm
```

For a manually operated workload, `python -m scripts.demo seed` creates 60 jobs
across three organizations and prints a run ID. Start two or three consumers
separately, then inspect the workload with the `report` command. Stop the
consumers with Ctrl+C. Do not seed a database used by unrelated consumers.

## Configuration

Configuration is read from environment variables. `.env` is not loaded
implicitly by the application; export it in each shell as shown above.

| Variable | Default | Description |
| --- | --- | --- |
| `DATABASE_URL` | Required | SQLAlchemy `postgresql+psycopg` URL |
| `DB_POOL_SIZE` | `5` | SQLAlchemy connections per process |
| `DB_MAX_OVERFLOW` | `10` | Extra connections per process |
| `CONSUMER_ID` | Generated | Stable process identity for logs/ownership |
| `WORKER_CONCURRENCY` | `4` | Maximum worker threads per consumer |
| `POLL_INTERVAL_SECONDS` | `0.5` | Idle polling interval |
| `CLAIM_BATCH_SIZE` | `10` | Maximum claim batch; also bounded by free worker slots |
| `LEASE_SECONDS` | `30` | Claim lease and worker heartbeat interval basis |
| `SHUTDOWN_GRACE_SECONDS` | `20` | Grace period before waiting for in-flight work |
| `DB_RECONNECT_BASE_SECONDS` | `0.5` | Initial database reconnect delay |
| `DB_RECONNECT_MAX_SECONDS` | `15` | Maximum database reconnect delay |
| `RETRY_BASE_SECONDS` | `1` | Initial retry/recovery delay |
| `RETRY_MAX_SECONDS` | `60` | Maximum handler retry delay |
| `RETRY_JITTER_RATIO` | `0.25` | Retry delay jitter from `0` to `1` |
| `LEASE_REAP_INTERVAL_SECONDS` | `5` | Expired-lease recovery polling interval |

Docker Compose also reads `POSTGRES_DB`, `POSTGRES_USER`,
`POSTGRES_PASSWORD`, and `POSTGRES_HOST_PORT` from `.env`; keep these consistent
with the local `DATABASE_URL`.

## Tests

Run unit tests without a database:

```powershell
python -m pytest tests/unit -q
```

Integration tests use PostgreSQL-specific locking and behavior; SQLite is not a
substitute. Point `TEST_DATABASE_URL` at a **dedicated test database**. The
fixture applies migrations but does not delete unrelated database contents:

```powershell
$env:TEST_DATABASE_URL = "******localhost:5432/scheduler_test"
python -m pytest -q
```

When `TEST_DATABASE_URL` is unset, PostgreSQL integration tests are skipped.
The suite covers persistence, concurrent claims/processes, tenant limits,
scheduling boundaries, retries, lease recovery, stale-claim rejection,
consumer restarts, and idempotent sample operations.

## Architecture and limitations

- **Persistence and coordination:** PostgreSQL stores jobs and coordinates
  claims using transactions, indexes, row-level locks, and leases.
- **Execution:** Consumer processes poll independently; each dispatches to a
  bounded thread pool. Handler execution occurs outside the claim transaction.
- **Scheduling:** Due times use database time. Persistent unweighted
  round-robin gives eligible organizations dispatch turns while enforcing
  database-backed concurrency caps. It does not guarantee equal completion
  times or strict latency.
- **Retries and recovery:** Attempts are bounded; expired leases are recoverable
  after a consumer restart. Lease expiration does not stop an old worker.
- **Scaling:** More consumers increase database connection and lock demand.
  PostgreSQL remains the shared throughput limit; measure query plans,
  contention, write/index overhead, and connection capacity before scaling.

See [the design document](docs/design.md) for the trade-offs, idempotency
guarantees, fairness policy, and 10x/100x scaling considerations.
