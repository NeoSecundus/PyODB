from src.pyodb.schema._table import Table


class TableSchema:
    _tables: dict[type, Table] = {}

    @classmethod
    def is_known_type(cls, obj_type: type) -> bool:
        """Check whether the type is already defined in the schema

        Args:
            obj_type (type): Type to check for a schema definition

        Returns:
            bool: True if found, otherwise False
        """
        if obj_type in cls._tables:
            return True
        return False


    @classmethod
    def add_type(cls, obj_type: type):
        pass


    @classmethod
    @property
    def schema_size(cls) -> int:
        """Number of table definitions / types in the current schema"""
        return len(cls._tables)
