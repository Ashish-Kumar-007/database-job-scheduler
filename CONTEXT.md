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
