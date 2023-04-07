from src.pyodb.schema._base_schema import BaseSchema
from src.pyodb.schema.base._operators import Disassembler
from src.pyodb.schema.base._table import Table


class ShardSchema(BaseSchema):
    def add_type(self, base_type: type):
        tables = Disassembler.disassemble_type(base_type)
        for table in tables:
            if self.is_known_type(table.base_type):
                continue
            table.dbconn = self._create_dbconn(self._base_path / f"{table.name}.db")
            self._tables[table.base_type] = table
            table.create_table()
        self._tables[base_type].is_parent = True


    def load_existing(self):
        self.add_type(Table)
        old_tables: list[Table] = self.select(Table).all()
        for old_table in old_tables:
            self.add_type(old_table.base_type)
            self._tables[old_table.base_type].dbconn = self._create_dbconn(
                self._base_path / f"{old_table.name}.db"
            )
            self._tables[old_table.base_type].is_parent = old_table.is_parent


    def _save_schema(self):
        self.add_type(Table)
        self.delete(Table).commit()
        self.max_depth = 0
        self.insert_many(list(self._tables.values()), None)


    def __del__(self):
        if self.is_persistent:
            self._save_schema()
            return

        for table in self._tables.values():
            del table.dbconn
            (self._base_path / (table.name + ".db")).unlink(True)
            (self._base_path / (table.name + ".db-shm")).unlink(True)
            (self._base_path / (table.name + ".db-wal")).unlink(True)
