# Job Scheduling

This context describes the tenants and durable units of work managed by the scheduler.

## Language

**Organization**:
A tenant that owns jobs and has a concurrency limit and scheduling weight.
_Avoid_: Customer, account

**Job**:
A durable request for a registered type of work, owned by one organization and eligible no earlier than its scheduled time.
_Avoid_: Task, message

**Attempt**:
One committed claim of a job for execution; a claim that later expires still counts toward the job's attempt limit.
_Avoid_: Retry count, worker start count

**Claim**:
Temporary ownership of a job by one consumer, identified by a unique token and bounded by a lease.
_Avoid_: Reservation, lock

**Lease**:
A time-limited period during which a claim remains valid; expiration allows the unfinished job to be recovered but does not prove its previous worker stopped.
_Avoid_: Lock timeout, execution deadline

**Recovery**:
Making an unfinished job eligible for another attempt after its claim lease expires.
_Avoid_: Cancellation, rollback

**Idempotency key**:
A stable identifier for one logical operation that lets a destination recognize repeated attempts as the same operation.
_Avoid_: Claim token, attempt identifier
