from pathlib import Path

from src.pyodb.schema._base_schema import BaseSchema
from src.pyodb.schema.base._operators import Disassembler
from src.pyodb.schema.base._table import Table


class UnifiedSchema(BaseSchema):
    def __init__(self, base_path: Path, max_depth: int, persistent: bool) -> None:
        self._dbconn = self._create_dbconn(base_path / "pyodb.db")
        super().__init__(base_path, max_depth, persistent)


    def add_type(self, base_type: type):
        tables = Disassembler.disassemble_type(base_type)
        for table in tables:
            if self.is_known_type(table.base_type):
                continue
            self._tables[table.base_type] = table
            table.dbconn = self._dbconn
            table.create_table()
        self._tables[base_type].is_parent = True


    def load_existing(self):
        self.add_type(Table)
        old_tables: list[Table] = self.select(Table).all()
        for old_table in old_tables:
            self.add_type(old_table.base_type)
            self._tables[old_table.base_type].dbconn = self._dbconn
            self._tables[old_table.base_type].is_parent = old_table.is_parent


    def _save_schema(self):
        self.add_type(Table)
        self._dbconn = None
        self.delete(Table).commit()
        self.max_depth = 0
        self.insert_many(list(self._tables.values()), None)


    def __del__(self):
        if self.is_persistent:
            self._save_schema()
            return

        del self._tables
        (self._base_path / "pyodb.db").unlink(True)
        (self._base_path / "pyodb.db-shm").unlink(True)
        (self._base_path / "pyodb.db-wal").unlink(True)
