from pyodb.schema._base_schema import BaseSchema
from pyodb.schema.base._operators import Disassembler
from pyodb.schema.base._table import Table


class UnifiedSchema(BaseSchema):
    def add_type(self, base_type: type):
        ttypes = Disassembler.disassemble_type(base_type)
        for ttype, members in ttypes.items():
            if self.is_known_type(ttype):
                continue
            self._tables[ttype] = Table(ttype, self._base_path, members, False)
            self._tables[ttype].create_table()
        self._tables[base_type].is_parent = True


    def load_existing(self) -> None:
        self.add_type(Table)
        old_tables: list[Table] = self.select(Table).all()
        for old_table in old_tables:
            self.add_type(old_table.base_type)
            self._tables[old_table.base_type].is_parent = old_table.is_parent


    def _save_schema(self) -> None:
        self.add_type(Table)
        self._dbconn = None
        self.delete(Table).commit()
        self.max_depth = 0
        self.insert_many(list(self._tables.values()), None)


    def __del__(self):
        if self.is_persistent:
            if self.save_table_defs:
                self._save_schema()
            return

        del self._tables
        (self._base_path / "pyodb.db").unlink(True)
        (self._base_path / "pyodb.db-shm").unlink(True)
        (self._base_path / "pyodb.db-wal").unlink(True)
