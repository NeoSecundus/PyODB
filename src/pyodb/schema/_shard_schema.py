import sqlite3.dbapi2 as sql

from src.pyodb.schema._base_schema import BaseSchema
from src.pyodb.schema.base._operators import Disassembler
from src.pyodb.schema.base._table import Table


class ShardSchema(BaseSchema):
    _tables: dict[type, tuple[sql.Connection, Table]]


    def add_type(self, base_type: type):
        tables = Disassembler.disassemble_type(base_type, self._max_depth)
        for table in tables:
            if not self.is_known_type(table.base_type):
                self._tables[table.base_type] = (
                    sql.connect(self._base_path / f"{table.base_name}.db"),
                    table
                )
        self._tables[base_type][1].is_parent = True


    def remove_type(self, base_type: type):
        if not self.is_known_type(base_type):
            raise TypeError(f"Tried to remove unknown type from schema: {base_type}")
        self._tables.pop(base_type)
