# Database-Backed Job Scheduler

This repository provides validated PostgreSQL connection settings, SQLAlchemy
database utilities, the `Organization` and `Job` models and migration, validated
job submission/retrieval, atomic claims, handler registration, and bounded
worker execution. The consumer polling loop, recovery scheduler, retry backoff,
and multi-tenant fairness are not implemented yet.

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

## Phase 3: atomic claims

`JobRepository.claim_due_jobs(owner, limit, lease_seconds)` claims only due
`scheduled` jobs and due `retry_wait` jobs. It checks eligibility against
PostgreSQL `clock_timestamp()`, locks eligible rows with
`FOR UPDATE SKIP LOCKED`, assigns a fresh claim token and lease, increments
`attempts`, then commits before returning claim snapshots. The batch size is
bounded to 100. A claim does not set `started_at`; that timestamp belongs to
the later worker-start transition. Tenant fairness and concurrency limits are
not yet enforced by claiming and are deferred to their dedicated phase.

## Phase 4: handlers and worker execution

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
failure are in `app/handlers/sample.py`. A worker pool can be used directly by
application code; the consumer process/polling loop is a later phase.

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

Consumer processes, atomic claiming, retries, recovery, and broader PostgreSQL
integration coverage will follow in later implementation phases.
