import math
import random


class RetryPolicy:
    def __init__(
        self,
        *,
        base_seconds: float,
        max_seconds: float,
        jitter_ratio: float = 0.0,
        random_source: random.Random | None = None,
    ) -> None:
        if (
            not math.isfinite(base_seconds)
            or not math.isfinite(max_seconds)
            or not math.isfinite(jitter_ratio)
            or base_seconds <= 0
            or max_seconds < base_seconds
            or not 0 <= jitter_ratio <= 1
        ):
            raise ValueError("invalid retry backoff configuration")
        self._base_seconds = base_seconds
        self._max_seconds = max_seconds
        self._jitter_ratio = jitter_ratio
        self._random = random_source or random.Random()

    def delay_seconds(self, attempt: int) -> float:
        if isinstance(attempt, bool) or not isinstance(attempt, int) or attempt < 1:
            raise ValueError("attempt must be a positive integer")
        max_exponent = math.ceil(
            math.log2(self._max_seconds) - math.log2(self._base_seconds)
        )
        capped = (
            self._max_seconds
            if attempt - 1 >= max_exponent
            else self._base_seconds * (2 ** (attempt - 1))
        )
        if self._jitter_ratio == 0:
            return capped
        lower = capped * (1 - self._jitter_ratio)
        upper = min(
            self._max_seconds,
            capped * (1 + self._jitter_ratio),
        )
        return self._random.uniform(lower, upper)
