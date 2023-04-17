class PyODBError(Exception):
    """Base class for all other custom exceptions. Provides dynamic logging!"""
    def __str__(self) -> str:
        return self.msg


    def __init__(self, msg: str) -> None:
        self.msg = msg
        super().__init__()


class ExpiryError(PyODBError):
    """Bad expiration date error."""

class MixedTypesError(PyODBError):
    """Custom and Primitive Type mixing error."""

class DisassemblyError(PyODBError):
    """Type Dissassembly Error."""

class ParentError(PyODBError):
    """Database bad/not a parent error."""

class QueryError(PyODBError):
    """Database Query Error."""

class UnknownTypeError(PyODBError):
    """Unknown Type Error."""

class BadTypeError(PyODBError):
    """Bad/Unexpected Type Error"""

class CacheError(PyODBError):
    """An error occured in a datacache or datacache function."""
