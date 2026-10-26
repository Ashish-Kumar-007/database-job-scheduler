# Scheduler design

## Persistence and atomic claiming

PostgreSQL is both the durable job store and the coordination mechanism. SQLAlchemy
2.x maps organizations and jobs; Alembic owns schema changes. PostgreSQL JSONB
stores handler payloads, UUIDs identify tenants and jobs, and timezone-aware
timestamps are compared in UTC. Partial indexes support scheduled jobs, retry
eligibility, and lease expiration.

Consumers claim small batches in short transactions. A claim transaction checks
due times using PostgreSQL `clock_timestamp()`, locks eligible organizations
and jobs with `FOR UPDATE SKIP LOCKED`, changes the job to `running`, increments
its attempt count, assigns a fresh claim token, and sets a lease. The commit
finishes before dispatch; handler work never holds the claim transaction open.
Start, completion, failure, and lease-renewal updates require the current token
and an unexpired lease, so an old worker cannot overwrite newer scheduler state.

## Leases, retries, and delivery guarantees

Workers heartbeat long-running claims. Consumers recover expired leases under
row locks, clear stale ownership, and either schedule another attempt or
terminally fail an exhausted job. Attempts count committed claims, including
claims whose worker crashes before starting. Handler failures use capped
exponential backoff with configurable jitter; permanent failures do not retry.

The system provides recoverable, at-least-once processing and at most one valid
claim at a time. Lease expiry does not prove that a previous worker stopped.
Claim tokens fence database updates but cannot cancel external side effects.
Therefore arbitrary external operations are not exactly once: a crash after an
external effect but before recording completion can repeat that effect. The
sample idempotent handler writes an operation to PostgreSQL under a unique job
identifier, so repeat attempts return the original result instead of inserting
a second operation. External handlers should pass the stable job UUID as an
idempotency key to destinations that support it.

## Tenant fairness and scheduling

The claim policy is persistent, unweighted round-robin by each organization's
last claim time. A turn claims at most one job for a tenant; never-served
tenants are ordered first. The organization row lock serializes capacity
decisions across consumer processes. Unexpired running claims count against
the tenant's concurrency limit, and each tenant may have at most one unstarted
claim, preventing batch prefetch from reserving its capacity.

This is dispatch fairness, not equal execution time, throughput, or completion
latency. A tenant with long-running jobs can remain at its concurrency cap.
Organization weights are stored but not used by this unweighted policy.
Initial claims require `scheduled_at <=` database time; retries additionally
require `next_attempt_at <=` database time. Start time minus original scheduled
time measures scheduling delay; retry wait is separately measured from the
retry eligibility timestamp.

## Operations, observability, and scale

Structured lifecycle logs include job, organization, consumer, attempt, outcome,
and relevant timing fields without logging payloads. Consumer summaries include
outcome/recovery counters, throughput, and average, maximum, and sampled p95
scheduling delay. The reproducible demo seeds 60 varied jobs across three
tenants, adds an isolated crash-recovery job, and runs three consumer processes.

At 10x load, first increase consumer replicas and bounded worker slots only
within PostgreSQL connection and CPU budgets. Tune claim batches and polling
intervals using measured throughput and query plans; keep batches bounded to
free worker capacity. At 100x, measure lock contention, write/index overhead,
connection saturation, and fairness-query cost. Partitioning large lifecycle
tables or sharding tenants may become appropriate, but both complicate
cross-tenant scheduling and recovery. More consumers do not provide unlimited
throughput because PostgreSQL remains the shared coordination point.

With more time, useful extensions include weighted fairness with persisted
deficit accounting, per-attempt history, structured metrics export, and
performance tests at larger data volumes. Any such change should preserve
database-time eligibility, token fencing, and the stated at-least-once
guarantee.
