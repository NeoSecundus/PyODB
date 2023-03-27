from types import UnionType

from src.pyodb.schema._base_schema import BaseSchema
from src.pyodb.schema.components._table import Table


class UnifiedSchema(BaseSchema):
    _tables: dict[type, Table]

    def add_type(self, base_type: type):
        tables = self._disassemble_type(base_type)
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
