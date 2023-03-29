import sqlite3.dbapi2 as sql
from pathlib import Path

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
            table.dbconn = self._dbconn
            if not self.is_known_type(table.base_type):
                self._tables[table.base_type] = table
        self._tables[base_type].is_parent = True
