import sqlite3.dbapi2 as sql
from pathlib import Path
from types import UnionType

from src.pyodb.schema._base_schema import BaseSchema
from src.pyodb.schema.base._operators import Disassembler
from src.pyodb.schema.base._table import Table


class UnifiedSchema(BaseSchema):
    _tables: dict[type, Table]
    _dbconn: sql.Connection


    def __init__(self, base_path: Path, max_depth: int) -> None:
        self._dbconn = sql.connect(base_path.as_posix(), check_same_thread=True)
        super().__init__(base_path, max_depth)


    def add_type(self, base_type: type):
        tables = Disassembler.disassemble_type(base_type, self._max_depth)
        for table in tables:
            if not self.is_known_type(table.base_type):
                self._tables[table.base_type] = table
        self._tables[base_type].is_parent = True


    def remove_type(self, base_type: type):
        if not self.is_known_type(base_type):
            raise TypeError(f"Cannot remove type! Unknown type: {base_type}")
        self._remove_type(base_type)


    def _remove_type(self, base_type: type):
        if self._tables[base_type].is_parent:
            return

        table = self._tables.pop(base_type)
        for _, type_ in table.members.items():
            if isinstance(type_, UnionType):
                types = type_.__args__
                for sub_type in types:
                    if self.is_known_type(sub_type):
                        self._remove_type(sub_type)
            elif self.is_known_type(type_):
                self._remove_type(type_)
