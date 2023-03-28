from logging import Logger
from pathlib import Path

from src.pyodb.schema.base._operators import Disassembler


class BaseSchema:
    _tables: dict
    _base_path: Path
    logger: Logger | None


    def __init__(self, base_path: Path, max_depth: int) -> None:
        self._tables = {}
        self._max_depth = max_depth
        self._base_path = base_path
        Disassembler.max_depth = max_depth


    def is_known_type(self, obj_type: type) -> bool:
        """Check whether the type is already defined in the schema

        Args:
            obj_type (type): Type to check for a schema definition

        Returns:
            bool: True if found, otherwise False
        """
        return obj_type in self._tables


    def add_type(self, base_type: type):
        raise NotImplementedError()


    def remove_type(self, base_type: type):
        raise NotImplementedError()


    @property
    def max_depth(self) -> int:
        return Disassembler.max_depth


    @property
    def schema_size(self) -> int:
        """Number of table definitions / types in the current schema"""
        return len(self._tables)
