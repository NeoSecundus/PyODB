import sqlite3.dbapi2 as sql
from pathlib import Path

from src.pyodb.schema._base_schema import BaseSchema
from src.pyodb.schema.base._operators import Disassembler


class UnifiedSchema(BaseSchema):
    _dbconn: sql.Connection


    def __init__(self, base_path: Path, max_depth: int) -> None:
        self._dbconn = sql.connect((base_path / "pyodb.db").as_posix(), check_same_thread=True)
        self._dbconn.row_factory = sql.Row
        super().__init__(base_path, max_depth)


    def add_type(self, base_type: type):
        tables = Disassembler.disassemble_type(base_type)
        for table in tables:
            if self.is_known_type(table.base_type):
                continue
            self._tables[table.base_type] = table
            table.dbconn = self._dbconn
            table.create_table()
        self._tables[base_type].is_parent = True
