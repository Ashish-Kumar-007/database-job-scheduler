class InputValidationError(ValueError):
    """Raised when a job or organization submission is invalid."""


class OrganizationNotFound(LookupError):
    """Raised when a job references an organization that does not exist."""
