import random

import pytest

from app.services.retry import RetryPolicy


def test_retry_policy_applies_capped_exponential_delays() -> None:
    policy = RetryPolicy(
        base_seconds=0.5,
        max_seconds=2,
        jitter_ratio=0,
    )

    assert [policy.delay_seconds(attempt) for attempt in range(1, 6)] == [
        0.5,
        1,
        2,
        2,
        2,
    ]


def test_retry_policy_jitter_stays_within_configured_bounds() -> None:
    policy = RetryPolicy(
        base_seconds=1,
        max_seconds=10,
        jitter_ratio=0.2,
        random_source=random.Random(7),
    )

    delay = policy.delay_seconds(2)

    assert 1.6 <= delay <= 2.4


@pytest.mark.parametrize(
    ("base", "maximum", "jitter"),
    [(0, 1, 0), (2, 1, 0), (1, 2, -0.1), (float("nan"), 2, 0)],
)
def test_retry_policy_rejects_invalid_configuration(base, maximum, jitter) -> None:
    with pytest.raises(ValueError):
        RetryPolicy(
            base_seconds=base,
            max_seconds=maximum,
            jitter_ratio=jitter,
        )


@pytest.mark.parametrize("attempt", [0, -1, True, 1.5])
def test_retry_policy_requires_positive_integer_attempt(attempt) -> None:
    policy = RetryPolicy(base_seconds=1, max_seconds=4)

    with pytest.raises(ValueError):
        policy.delay_seconds(attempt)
