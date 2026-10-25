# Scheduling design

## Multi-tenant dispatch

The scheduler uses persistent, unweighted round-robin dispatch. Each claim
transaction selects the eligible organization with the oldest
`last_claimed_at`, ordering never-served organizations first and using the UUID
as a deterministic tie-breaker. A turn claims at most one due job for that
organization; a job's scheduled timestamp orders work within its tenant.
Repeated turns can serve a tenant again after other currently eligible
organizations have had an opportunity.

The organization row is locked with `FOR UPDATE SKIP LOCKED` while its
available capacity is checked and a job is claimed. This serializes claim
decisions for a tenant across independent consumers. Available capacity is
`max_concurrent_jobs` minus the number of unexpired `running` claims. Each
tenant may have at most one unstarted claim, preventing a claim batch from
reserving multiple worker slots for that tenant before dispatch begins.
Expired leases do not count as valid active claims and are independently
recovered by the lease reaper. The existing organization `weight` setting is
not applied by this unweighted policy.

## Fairness boundaries

This policy provides **dispatch fairness**: eligible organizations take turns
receiving claim opportunities. It does not provide equal start latency,
execution time, throughput, or completion time. A tenant whose active jobs are
long-running can remain at its concurrency limit while other tenants finish
more jobs. Scheduling order is bounded by database polling and lock
availability, not by a strict latency guarantee. Increasing consumer count
does not remove the PostgreSQL coordination and connection limits.
