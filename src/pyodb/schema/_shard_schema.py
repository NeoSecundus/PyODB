from src.pyodb.schema._base_schema import BaseSchema
from src.pyodb.schema.components._table import Table


class ShardSchema(BaseSchema):
    _tables: dict[type, list[Table]]

    def add_type(self, base_type: type):
        self._tables[base_type] = self._disassemble_type(base_type)
        self._tables[base_type][0].is_parent = True


    def remove_type(self, base_type: type):
        raise NotImplementedError()
