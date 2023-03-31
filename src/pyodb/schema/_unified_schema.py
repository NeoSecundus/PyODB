import sqlite3.dbapi2 as sql

from src.pyodb.schema._base_schema import BaseSchema
from src.pyodb.schema.base._operators import Disassembler


class UnifiedSchema(BaseSchema):
    SAVE_NAME = "unified_schema"

    def add_type(self, base_type: type):
        dbconn = sql.connect((self._base_path / "pyodb.db").as_posix(), check_same_thread=True)
        dbconn.row_factory = sql.Row
        tables = Disassembler.disassemble_type(base_type)
        for table in tables:
            if self.is_known_type(table.base_type):
                continue
            self._tables[table.base_type] = table
            table.dbconn = dbconn
            table.create_table()
        self._tables[base_type].is_parent = True


    def __del__(self):
        if self.is_persistent:
            self._save_schema()
            return

        del self._tables
        (self._base_path / "pyodb.db").unlink(True)
        (self._base_path / self.SAVE_NAME).unlink(True)


    def __setstate__(self, state: dict):
        self.__dict__ |= state
        for table in self._tables.values():
            table.dbconn = sql.connect(self._base_path / "pyodb.db")
        self.logger = None
