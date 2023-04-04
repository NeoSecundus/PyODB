"""The main module which processes python primitives (and lists and dicts) and generates sql
statements.
Including create and remove table statements in case the fields contained by a class were changed.
"""
import sqlite3 as sql
from types import GenericAlias, NoneType, UnionType

from src.pyodb.error import DBConnError
from src.pyodb.schema.base._type_defs import BASE_TYPE_SQL_MAP, BASE_TYPES


class Table:
    _members: dict[str, type | UnionType | GenericAlias]
    base_type: type
    is_parent: bool
    dbconn: sql.Connection | None


    def __init__(self, base_type: type, is_parent: bool = False) -> None:
        self._members = {}
        self.base_type = base_type
        self.is_parent = is_parent
        self.dbconn = None


    def add_member(self, name: str, type_: type | UnionType | GenericAlias):
        """Adds a new member to the internal members

        Args:
            name (str): Name of the member (field name)
            type_ (type): Member's datatype
        """
        self._members[name] = type_


    @property
    def members(self) -> dict[str, type | UnionType | GenericAlias]:
        return self._members


    @property
    def name(self) -> str:
        return self.base_type.__name__


    @property
    def fqcn(self) -> str:
        """Fully qualified class name"""
        return f"{self.base_type.__module__}.{self.base_type.__name__}"


    def create_table(self):
        if not self.dbconn:
            raise DBConnError("Table has no valid connection to any Database!")
        self.dbconn.execute(self._create_table_sql())
        self.dbconn.commit()


    def drop_table(self):
        if not self.dbconn:
            raise DBConnError("Table has no valid connection to any Database!")
        self.dbconn.execute(self._drop_table_sql())
        self.dbconn.commit()


    def delete_parent_entries(self, parent):
        if not self.dbconn:
            raise DBConnError("Table has no valid connection to any Database!")
        self.dbconn.execute(f"DELETE FROM \"{self.fqcn}\" WHERE _parent_table_ = '{parent.name}'")
        self.dbconn.commit()


    def _create_table_sql(self) -> str:
        sql = f"CREATE TABLE IF NOT EXISTS \"{self.fqcn}\" (_uid_ TEXT PRIMARY KEY,_parent_ TEXT,\
_parent_table_ TEXT,_expires_ REAL,"
        for name, type_ in self.members.items():
            if isinstance(type_, (GenericAlias, UnionType)):
                type_ = self._get_base_type(type_) # noqa: PLW2901

            if type_ in BASE_TYPES:
                sql += f"{name} {BASE_TYPE_SQL_MAP[type_]},"
            else:
                sql += f"{name} TEXT,"

        return sql[:-1] + ");"


    @classmethod
    def _get_base_type(cls, type_: GenericAlias | UnionType) -> type | UnionType:
        if isinstance(type_, UnionType):
            if type_ in BASE_TYPES:
                return type_

            ret = type_
            for t in type_.__args__:
                if isinstance(t, GenericAlias):
                    ret = cls._get_base_type(t)
                    continue

                if isinstance(ret, type) and t is NoneType:
                    ret = ret | None
            return ret
        else:
            return type_.__origin__


    def _drop_table_sql(self) -> str:
        return f"DROP TABLE IF EXISTS \"{self.fqcn}\";"


    def __repr__(self) -> str:
        return f"{self.base_type.__name__}: \
{ {k: str(t) if isinstance(t, UnionType) else t.__name__ for k, t in self._members.items()} };"
