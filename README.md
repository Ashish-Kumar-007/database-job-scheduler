# Database-Backed Job Scheduler

This repository provides validated PostgreSQL connection settings, SQLAlchemy
database utilities, the `Organization` and `Job` models and migration, validated
job submission/retrieval, atomic claims, handler registration, bounded worker
execution, and an independent consumer polling process. Lease recovery,
retry backoff, and multi-tenant fairness are not implemented yet.

## Requirements

- Python 3.12 or later
- Docker Engine with Docker Compose

## Start PostgreSQL

From the project directory in PowerShell:

```powershell
Copy-Item .env.example .env
docker compose up -d postgres
docker compose ps
```

The Compose service uses PostgreSQL 16. The sample credentials are for local
development only. To select another host port, set `POSTGRES_HOST_PORT` in
`.env`; update the port in `DATABASE_URL` to match.

## Install and check the database

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[dev]"
$env:DATABASE_URL = (Get-Content .env | Where-Object { $_ -match '^DATABASE_URL=' } | Select-Object -First 1) -replace '^DATABASE_URL=', ''
python -m app
```

The health check executes `SELECT 1`. Configuration requires a SQLAlchemy
PostgreSQL psycopg URL with host, database, username, and password. Credentials
are parsed as a URL rather than interpolated into a connection string.

## Database schema

Apply the initial schema migration after PostgreSQL is healthy:

```powershell
python -m alembic upgrade head
```

The schema includes tenant/job validation constraints and partial indexes for
scheduled jobs, retry eligibility, and expired leases. `attempts` counts
committed claims, including claims later recovered after lease expiry; waiting
jobs must have an attempt remaining. Timestamps are stored as timezone-aware
PostgreSQL timestamps.

## Atomic claims

`JobRepository.claim_due_jobs(owner, limit, lease_seconds)` claims only due
`scheduled` jobs and due `retry_wait` jobs. It checks eligibility against
PostgreSQL `clock_timestamp()`, locks eligible rows with
`FOR UPDATE SKIP LOCKED`, assigns a fresh claim token and lease, increments
`attempts`, then commits before returning claim snapshots. The batch size is
bounded to 100. A claim does not set `started_at`; that timestamp belongs to
the later worker-start transition. Tenant fairness and concurrency limits are
not yet enforced by claiming and are deferred to their dedicated phase.

## Handlers and worker execution

Register a callable per `job_type` with `HandlerRegistry`. Handlers receive the
job UUID, JSON payload, organization ID context, and idempotency context. The
idempotency key is the stable job UUID across attempts; handlers integrating
external systems must pass that key to a destination that supports
idempotency. Claim tokens fence database status updates, but cannot undo or
prevent duplicate external side effects.

`WorkerPool` uses a bounded thread pool and rejects dispatch when all worker
slots are occupied rather than building an unbounded local queue. Before
calling a handler, it verifies and records the claim start using the current
lease token. Completion and failure updates require the same token and an
unexpired lease. Handler failures are recorded and immediately made retryable
while attempts remain; otherwise they become terminal. Exponential backoff
and recovery are deferred to later phases. Worker sessions are not shared:
each repository operation opens its own session/transaction.

Sample handlers for deterministic success, configurable delay, and controlled
failure are in `app/handlers/sample.py`. Worker threads use independent
repository sessions; no SQLAlchemy session is passed to a handler.

## Run consumer processes

After installing the project and applying migrations, start a consumer in each
PowerShell terminal:

```powershell
python -m app.consumers
```

Each process has an independent database engine/session factory, consumer
identity, and bounded worker pool. Set `WORKER_CONCURRENCY`,
`POLL_INTERVAL_SECONDS`, `CLAIM_BATCH_SIZE`, `LEASE_SECONDS`, and
`SHUTDOWN_GRACE_SECONDS` in the environment or `.env`-derived environment before
starting it. `CLAIM_BATCH_SIZE` is additionally capped to currently available
worker slots. Press Ctrl+C to stop polling and drain in-flight handlers.

For a bounded run or process-level smoke test, pass `--max-completed N`. The
CLI registers `sample.success`, `sample.slow`, and `sample.failure` handlers.
Database connection failures use capped exponential backoff with jitter.
Run `python -m app.consumers` in two or three terminals to start independent
processes. Leave `CONSUMER_ID` empty so each derives a unique
hostname/process/random identity. Logs include that identity, claimed job IDs,
outcomes, and a stop summary. Claiming is capped by both configured batch size
and currently free worker slots, so the consumer does not intentionally
prefetch work beyond its local execution capacity.

The sample `.env` values are not loaded implicitly. In each PowerShell
terminal, export the entries before starting the consumer:

```powershell
Get-Content .env | Where-Object { $_ -match '^[A-Z_]+=' } | ForEach-Object {
    $name, $value = $_ -split '=', 2
    Set-Item -Path "Env:$name" -Value $value
}
python -m app.consumers
```

Stop PostgreSQL with `docker compose down`. Its named volume preserves data;
`docker compose down --volumes` removes the local database volume.

## Tests

```powershell
python -m pytest
```

The PostgreSQL persistence test uses a separate database URL, applies pending
migrations, and verifies that records remain available to a fresh application
process. Point `TEST_DATABASE_URL` at a
dedicated test database; do not point it at a database containing unrelated
data:

```powershell
$env:TEST_DATABASE_URL = "postgresql+psycopg://scheduler:local-development-only@localhost:5432/scheduler_test"
python -m pytest
```

Without `TEST_DATABASE_URL`, the PostgreSQL persistence test is skipped. The
migration can be rendered without a live database for review:

```powershell
python -m alembic upgrade head --sql
```

Lease recovery, exponential retry backoff, tenant fairness, and broader
PostgreSQL integration coverage remain for later implementation phases.
