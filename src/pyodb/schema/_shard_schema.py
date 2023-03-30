import sqlite3.dbapi2 as sql

from src.pyodb.schema._base_schema import BaseSchema
from src.pyodb.schema.base._operators import Disassembler


class ShardSchema(BaseSchema):
    def add_type(self, base_type: type):
        tables = Disassembler.disassemble_type(base_type)
        for table in tables:
            if not self.is_known_type(table.base_type):
                table.dbconn = sql.connect(self._base_path / f"{table.name}.db")
                self._tables[table.base_type] = table
        self._tables[base_type].is_parent = True
