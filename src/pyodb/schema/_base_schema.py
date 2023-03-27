from logging import Logger
from types import GenericAlias, UnionType
from typing import Any

from src.pyodb.schema.components._table import PRIMITIVES, Table


class BaseSchema:
    _tables: dict
    _max_depth: int
    logger: Logger | None

    def __init__(self, logger: Logger | None, max_depth: int) -> None:
        self._tables = {}
        self.logger = logger
        self._max_depth = max_depth


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
    def schema_size(self) -> int:
        """Number of table definitions / types in the current schema"""
        return len(self._tables)


    def _disassemble_type(self, obj_type: type, depth: int = 0) -> list[Table]:
        if depth > self._max_depth:
            return []

        if obj_type is Any:
            raise TypeError("'Any' type is not supported!")

        if not isinstance(obj_type, type):
            raise TypeError("Passed argument must be a type!")

        if obj_type in PRIMITIVES:
            raise TypeError("Object type to disassemble cannot be a primitive type")
        tables = [Table(obj_type)]

        members: dict[str, type | UnionType] = {
            key: getattr(type_, "__origin__", None) or type_
            for key, type_
            in obj_type.__annotations__.items()
            if key[0] != "__"
        }

        for key, type_ in members.items():
            tables[0].add_member(key, type_)
            if type_ not in PRIMITIVES:
                if isinstance(type_, UnionType):
                    if any([t in PRIMITIVES for t in type_.__args__]):
                        raise TypeError(
                            f"Cannot save object with mixed primitive and complex type annotations \
or multiple primitives! Got: {type_}"
                        )
                    for t in type_.__args__:
                        tables += self._disassemble_type(t, depth+1)
                elif not isinstance(type_, GenericAlias):
                    tables += self._disassemble_type(type_, depth+1)

        return tables
