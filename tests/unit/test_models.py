from sqlalchemy.dialects.postgresql import JSONB

from app.db.base import Base
from app.models import Job, JobStatus, Organization, SampleOperation


def test_domain_models_register_expected_tables_and_columns() -> None:
    assert set(Base.metadata.tables) == {
        "organizations",
        "jobs",
        "sample_operations",
    }
    assert isinstance(Job.__table__.c.payload.type, JSONB)
    assert Job.__table__.c.scheduled_at.type.timezone is True
    assert Job.__table__.c.lease_expires_at.type.timezone is True
    organization_fk = next(iter(Job.__table__.c.organization_id.foreign_keys))
    assert organization_fk.target_fullname == "organizations.id"
    assert organization_fk.ondelete == "RESTRICT"
    assert Organization.__table__.c.id.primary_key
    assert Organization.__table__.c.last_claimed_at.nullable
    assert SampleOperation.__table__.c.job_id.primary_key
    assert SampleOperation.__table__.c.idempotency_key.unique


def test_job_statuses_and_database_invariants_are_declared() -> None:
    assert {status.value for status in JobStatus} == {
        "scheduled",
        "running",
        "retry_wait",
        "completed",
        "failed",
    }
    job_constraints = {constraint.name for constraint in Job.__table__.constraints}
    organization_constraints = {
        constraint.name for constraint in Organization.__table__.constraints
    }
    assert {
        "ck_jobs_attempts_within_limit",
        "ck_jobs_max_attempts_positive",
        "ck_jobs_waiting_has_attempts_remaining",
        "ck_jobs_claim_metadata_matches_status",
        "ck_jobs_retry_schedule_matches_status",
        "ck_jobs_completion_matches_status",
    } <= job_constraints
    assert {
        "ck_organizations_name_nonempty",
        "ck_organizations_max_concurrent_jobs_positive",
        "ck_organizations_weight_positive",
    } <= organization_constraints
    assert {index.name for index in Job.__table__.indexes} == {
        "ix_jobs_organization_id",
        "ix_jobs_scheduled_due",
        "ix_jobs_retry_due",
        "ix_jobs_expired_lease",
    }
