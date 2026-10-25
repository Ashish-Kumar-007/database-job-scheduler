# Database-Backed Job Scheduler

This repository provides validated PostgreSQL settings, SQLAlchemy models and
migrations, job submission and retrieval, atomic claims, handler registration,
bounded worker execution, independent consumer processes, lease renewal and
recovery, bounded retries, and a durable idempotent sample operation.
Multi-tenant fair scheduling is not implemented yet.

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

Apply the schema migrations after PostgreSQL is healthy:

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
PostgreSQL `clock_timestamp()`, locks organizations and jobs with
`FOR UPDATE SKIP LOCKED`, assigns a fresh claim token and lease, increments
`attempts`, then commits before returning claim snapshots. The batch size is
bounded to 100. A claim does not set `started_at`; that timestamp belongs to
the later worker-start transition.

Eligible organizations are selected in persistent round-robin order by
oldest `last_claimed_at` (never-served organizations first); each scheduling
turn claims at most one job from that organization. The organization row lock
serializes capacity checks across consumer processes. A tenant is eligible
only while its unexpired running claims are below `max_concurrent_jobs`, and
while it has no unstarted claim. This permits at most one claimed-but-not-yet-
started job per organization, independent of batch size. Per-organization
limits are configured through `Organization.max_concurrent_jobs`.

This provides dispatch fairness, not equal execution or completion times:
jobs differ in duration, and long-running jobs continue to consume their
tenant's active capacity. The policy does not promise strict latency bounds.
See [the scheduling design](docs/design.md) for the policy and its fairness
boundaries.

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
unexpired lease. A separate heartbeat renews active leases every third of the
configured lease duration. Consumers periodically recover expired claims with
`FOR UPDATE SKIP LOCKED`; recovery clears stale ownership and either schedules
a retry or marks the job failed when attempts are exhausted. Attempts count
committed claims, so a worker killed after claim still consumes an attempt.
The first execution records `started_at - scheduled_at` as scheduling delay;
retry executions instead record elapsed time since their `next_attempt_at`,
keeping retry waiting distinct from the original scheduling delay.

Retryable handler failures use capped exponential backoff with configurable
jitter (`RETRY_BASE_SECONDS`, `RETRY_MAX_SECONDS`, and
`RETRY_JITTER_RATIO`). `RetryableJobError` requests a retry;
`PermanentJobError` and an unregistered handler type fail immediately.
Unclassified handler exceptions are considered retryable. Expired-lease
recovery waits the configured base retry delay. Worker sessions are not shared:
each repository operation opens its own session and transaction.

The `sample.idempotent` handler demonstrates a durable sample side effect in
the `sample_operations` table, uniquely keyed by job UUID. Repeated attempts
return the first stored result instead of inserting a second operation. This
guarantee applies only to effects committed through that database ledger. For
external APIs, pass the stable job UUID as the destination's idempotency key
when supported. A database claim token fences scheduler state changes but
cannot cancel a stale worker or make an arbitrary external effect exactly
once; a crash after an external effect and before completion can repeat it.

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
starting it. `DB_POOL_SIZE` and `DB_MAX_OVERFLOW` bound SQLAlchemy connections
per process; account for the sum across consumer replicas when setting the
database's connection budget. `RETRY_BASE_SECONDS`, `RETRY_MAX_SECONDS`,
`RETRY_JITTER_RATIO`, and `LEASE_REAP_INTERVAL_SECONDS` control retry and
recovery behavior. `CLAIM_BATCH_SIZE` is additionally capped to currently
available worker slots. Press Ctrl+C to stop polling and drain in-flight
handlers.

For a bounded run or process-level smoke test, pass `--max-completed N`. The
CLI registers `sample.success`, `sample.slow`, `sample.failure`, and
`sample.idempotent` handlers.
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

PostgreSQL integration tests use a separate database URL, apply pending
migrations, and verify persistence, concurrent claiming, tenant fairness,
lease recovery, process crashes, and idempotent operations. Point
`TEST_DATABASE_URL` at a dedicated test database; do not point it at a
database containing unrelated data:

```powershell
$env:TEST_DATABASE_URL = "postgresql+psycopg://scheduler:local-development-only@localhost:5432/scheduler_test"
python -m pytest
```

Without `TEST_DATABASE_URL`, the PostgreSQL persistence test is skipped. The
migration can be rendered without a live database for review:

```powershell
python -m alembic upgrade head --sql
```

PostgreSQL integration tests cover claims, leases, recovery, retries, process
crashes, tenant fairness and idempotent sample operations when
`TEST_DATABASE_URL` points to a dedicated PostgreSQL database.
