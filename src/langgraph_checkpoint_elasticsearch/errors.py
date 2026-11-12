class ScopeError(ValueError):
    """Raised when a scoped saver is called without the required tenant or user id."""
