from app.handlers.base import Handler


class UnknownJobType(LookupError):
    """Raised when no handler is registered for a job type."""


class HandlerRegistry:
    def __init__(self) -> None:
        self._handlers: dict[str, Handler] = {}

    def register(self, job_type: str, handler: Handler) -> None:
        if not isinstance(job_type, str) or not job_type.strip():
            raise ValueError("job_type must be a non-empty string")
        normalized_job_type = job_type.strip()
        if normalized_job_type in self._handlers:
            raise ValueError(f"Handler already registered for {normalized_job_type!r}")
        if not callable(handler):
            raise TypeError("handler must be callable")
        self._handlers[normalized_job_type] = handler

    def get(self, job_type: str) -> Handler:
        try:
            return self._handlers[job_type]
        except KeyError:
            raise UnknownJobType(f"No handler registered for {job_type!r}") from None
