from logging import Logger


class PyODBError(Exception):
    """Base class for all other custom exceptions. Provides dynamic logging!"""
    _logger: Logger | None = None


    @classmethod
    def set_logger(cls, logger: Logger | None):
        cls._logger = logger


    def __str__(self) -> str:
        return self.msg


    def __init__(self, msg: str) -> None:
        if self._logger:
            self._logger.error(self.__class__.__name__ + " -> " + msg)

        self.msg = msg
        super().__init__()


class DBConnError(PyODBError):
    """Database Connection Error."""

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
