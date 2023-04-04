import sqlite3.dbapi2 as sql

from src.pyodb.schema._base_schema import BaseSchema
from src.pyodb.schema.base._operators import Disassembler


class ShardSchema(BaseSchema):
    SAVE_NAME = "shard_schema"
    def add_type(self, base_type: type):
        tables = Disassembler.disassemble_type(base_type)
        for table in tables:
            if self.is_known_type(table.base_type):
                continue
            table.dbconn = self._create_dbconn(self._base_path / f"{table.name}.db")
            table.dbconn.row_factory = sql.Row
            self._tables[table.base_type] = table
            table.create_table()
        self._tables[base_type].is_parent = True


    def __del__(self):
        if self.is_persistent:
            self._save_schema()
            return

        for table in self._tables.values():
            del table.dbconn
            (self._base_path / (table.name + ".db")).unlink(True)
            (self._base_path / (table.name + ".db-shm")).unlink(True)
            (self._base_path / (table.name + ".db-wal")).unlink(True)
        (self._base_path / self.SAVE_NAME).unlink(True)


    def __setstate__(self, state: dict):
        self.__dict__ |= state
        self.logger = None
