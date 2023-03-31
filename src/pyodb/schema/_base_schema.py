import pickle
from logging import Logger
from pathlib import Path
from types import GenericAlias, UnionType
from typing import Any

from src.pyodb.schema.base._operators import Disassembler
from src.pyodb.schema.base._sql_builders import Delete, Insert, MultiInsert, Select
from src.pyodb.schema.base._table import Table
from src.pyodb.schema.base._type_defs import BASE_TYPES


class BaseSchema:
    SAVE_NAME: str = "base_schema"
    _tables: dict[type, Table]
    _base_path: Path
    _max_depth: int
    is_persistent: bool
    logger: Logger | None


    def __init__(self, base_path: Path, max_depth: int, persistent: bool) -> None:
        self._tables = {}
        self._max_depth = max_depth
        self._base_path = base_path
        self.is_persistent = persistent
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


    def load_existing(self):
        save_path = self._base_path / self.SAVE_NAME
        if save_path.exists():
            with save_path.open("rb") as save_file:
                schema = pickle.load(save_file)
                if isinstance(schema, BaseSchema):
                    self._tables = schema._tables


    def remove_type(self, base_type: type):
        if not self.is_known_type(base_type):
            raise TypeError(f"Cannot remove type! Unknown type: {base_type}")
        parent = self.get_parent(base_type)
        if parent:
            raise ConnectionError(
                f"'{base_type}' could not be removed because '{parent}' depends on it!"
            )
        self._remove_type(None, base_type)


    def _remove_type(self, previous: Table | None, base_type: type):
        if previous and (self.get_parent(base_type) or self._tables[base_type].is_parent):
            self._tables[base_type].delete_parent_entries(previous)
            return

        table = self._tables.pop(base_type)
        table.drop_table()
        for _, type_ in table.members.items():
            if isinstance(type_, UnionType):
                types = type_.__args__
                for sub_type in types:
                    if self.is_known_type(sub_type):
                        self._remove_type(table, sub_type)
            elif isinstance(type_, GenericAlias):
                for sbt in type_.__args__:
                    sbt = sbt.__args__ if isinstance(sbt, UnionType) else [sbt] # noqa: PLW2901
                    for t in sbt:
                        if self.is_known_type(t):
                            self._remove_type(table, t)
            elif self.is_known_type(type_):
                self._remove_type(table, type_)


    def get_parent(self, base_type: type) -> type | None:
        if not self.is_known_type(base_type):
            raise TypeError(f"Tried to get parent of unknown type: {base_type}")

        for ttype, table in self._tables.items():
            if ttype == base_type or not table.is_parent:
                continue

            for _, type_ in table.members.items():
                if type_ in BASE_TYPES:
                    continue

                if isinstance(type_, UnionType):
                    if any(base_type == t for t in type_.__args__):
                        return ttype
                    continue
                if isinstance(type_, GenericAlias):
                    for sbt in type_.__args__:
                        sbt = sbt.__args__ if isinstance(sbt, UnionType) else [sbt] # noqa: PLW2901
                        if any(base_type == t for t in sbt):
                            return ttype
                    continue
                elif type_ == base_type:
                    return ttype
        return None


    def insert(self, obj: object, expires: int | None, parent: Insert | None = None):
        if not self.is_known_type(type(obj)):
            raise TypeError(f"Tried to insert object of unknown type {type(obj)}")

        table = self._tables[type(obj)]
        if not table.dbconn:
            raise ConnectionError("Table has no valid connection to a database")

        if parent:
            inserter = Insert(table.fqcn, parent.uid, parent.table_name, expires)
        else:
            inserter = Insert(table.fqcn, None, None, expires)

        for member in table.members.keys():
            member = getattr(obj, member) # noqa: PLW2901
            inserter.add_val(member)
            if member and type(member) not in BASE_TYPES:
                self.insert(member, expires, inserter)
        inserter.commit(table.dbconn)


    def insert_many(self, objs: list, expires: int | None):
        base_type = type(objs[0])
        if not self.is_known_type(base_type):
            raise TypeError(f"Tried to insert object of unknown type {base_type}")

        table = self._tables[base_type]
        if not table.dbconn:
            raise ConnectionError("Table has no valid connection to a database")
        multi_inserter = MultiInsert(table.fqcn)

        if any(type(obj) != base_type for obj in objs):
            raise TypeError("Types in inserted list must all be the same!")

        for obj in objs:
            inserter = Insert(table.name, None, None, expires)

            for member in table.members.keys():
                member = getattr(obj, member) # noqa: PLW2901
                inserter.add_val(member)
                if member and type(member) not in BASE_TYPES:
                    self.insert(member, expires, inserter)
            multi_inserter += inserter
        multi_inserter.commit(table.dbconn)


    def select(self, type_: type) -> Select:
        if not self.is_known_type(type_):
            raise TypeError(f"Tried to select unknown type: {type_}")
        return Select(type_, self._tables)


    def delete(self, type_: type) -> Delete:
        if not self.is_known_type(type_):
            raise TypeError(f"Tried to delete instance of unknown type: {type_}")
        return Delete(type_, self._tables)


    def clear(self):
        for table in self._tables.values():
            if not table.dbconn or not table.is_parent:
                continue
            Delete(table.base_type, self._tables).commit()


    @property
    def max_depth(self) -> int:
        return Disassembler.max_depth


    @max_depth.setter
    def max_depth(self, val: int):
        if val < 0:
            raise ValueError("max_depth must be >= 0!")
        Disassembler.max_depth = val


    @property
    def schema_size(self) -> int:
        """Number of table definitions / types in the current schema"""
        return len(self._tables)


    def _save_schema(self):
        with open(self._base_path / self.SAVE_NAME, "wb") as file:
            pickle.dump(self, file)


    def __getstate__(self) -> dict[str, Any]:
        state = vars(self).copy()
        for key in state["_tables"].keys():
            state["_tables"][key].dbconn = None
        del state["logger"]
        return state
