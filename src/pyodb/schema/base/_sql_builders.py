import pickle
import sqlite3.dbapi2 as sql
from pydoc import locate
from time import time
from types import GenericAlias, NoneType, UnionType
from typing import Any

from src.pyodb._util import generate_uid
from src.pyodb.error import BadTypeError, DBConnError, ExpiryError, ParentError, QueryError
from src.pyodb.schema.base._operators import Assembler
from src.pyodb.schema.base._table import Table
from src.pyodb.schema.base._type_defs import BASE_TYPES, CONTAINERS, PRIMITIVES


class Insert:
    def __init__(
            self,
            table_name: str,
            parent: str | None,
            parent_table: str | None,
            expires: float | None
        ) -> None:
        self._table_name = table_name
        self._uid = generate_uid()
        if expires and expires <= time():
            raise ExpiryError("expires must greater than current timestamp")
        self._vals = [parent, parent_table, expires]


    def add_val(self, val: object):
        type_ = type(val)
        if type_ in PRIMITIVES or val is None:
            self._vals += [val]
        elif type_ in CONTAINERS:
            self._vals += [pickle.dumps(val)]
        else:
            self._vals += [f"{type_.__module__}.{type_.__name__}"]


    def commit(self, dbconn: sql.Connection):
        insert = f"INSERT INTO \"{self._table_name}\" VALUES("
        insert += "?,"*len(self.vals)
        insert = insert[:-1] + ");"
        dbconn.execute(insert, self.vals)
        dbconn.commit()


    @property
    def vals(self) -> list:
        return [self._uid] + self._vals

    @property
    def uid(self) -> str:
        return self._uid

    @property
    def table_name(self) -> str:
        return self._table_name


class MultiInsert:
    def __init__(self, table_name: str) -> None:
        self._vals: list[tuple] = []
        self._table_name = table_name


    def __add__(self, other: object):
        if isinstance(other, Insert):
            self._vals += [tuple(other.vals)]
        elif isinstance(other, MultiInsert):
            self._vals += other._vals
        else:
            raise BadTypeError(f"Cannot add {type(other)} to MultiInsert")
        return self


    def commit(self, dbconn: sql.Connection):
        insert = f"INSERT INTO \"{self._table_name}\" VALUES("
        insert += "?,"*len(self._vals[0])
        insert = insert[:-1] + ");"
        dbconn.executemany(insert, self._vals)
        dbconn.commit()


    @property
    def vals(self) -> list[tuple]:
        return self._vals


class _Query:
    class Where:
        def __init__(
                self,
                colname: str,
                operator: str,
                value: int|float|str|bool|None,
                or_: bool = False
            ) -> None:
            self.colname = colname
            self.operator = operator
            self.value = value
            self.connector = " OR " if or_ else " AND "


    _table: Table
    _tables: dict[type, Table]
    _wheres: list[Where]
    _limit: int | None
    _offset: int | None
    def __init__(self, type_: type, tables: dict[type, Table]) -> None:
        self._table = tables[type_]
        self._tables = tables
        self._wheres = []
        self._limit = None


    def eq(self, or_: bool = False, **kwargs):
        for key, val in kwargs.items():
            if not isinstance(val, (int, float, str, bool, NoneType)):
                raise BadTypeError(
                    f"Values must be int, float, str or bool for == check! Got: {type(val)}"
                )
            if val is None:
                self._wheres += [self.Where(key, " IS ", None, or_)]
            else:
                self._wheres += [self.Where(key, " = ", val, or_)]
        return self


    def ne(self, or_: bool = False, **kwargs):
        for key, val in kwargs.items():
            if not isinstance(val, (int, float, str, bool, NoneType)):
                raise BadTypeError(
                    f"Values must be int, float, str or bool for != check! Got: {type(val)}"
                )
            if val is None:
                self._wheres += [self.Where(key, " IS NOT ", None, or_)]
            else:
                self._wheres += [self.Where(key, " != ", val, or_)]
        return self


    def lt(self, or_: bool = False, **kwargs):
        for key, val in kwargs.items():
            if not isinstance(val, (int, float)):
                raise BadTypeError(f"Values must be int or float for < check! Got: {type(val)}")
            self._wheres += [self.Where(key, " < ", val, or_)]
        return self


    def gt(self, or_: bool = False, **kwargs):
        for key, val in kwargs.items():
            if not isinstance(val, (int, float)):
                raise BadTypeError(f"Values must be int or float for > check! Got: {type(val)}")
            self._wheres += [self.Where(key, " > ", val, or_)]
        return self


    def le(self, or_: bool = False, **kwargs):
        for key, val in kwargs.items():
            if not isinstance(val, (int, float)):
                raise BadTypeError(f"Values must be int or float for <= check! Got: {type(val)}")
            self._wheres += [self.Where(key, " <= ", val, or_)]
        return self


    def ge(self, or_: bool = False, **kwargs):
        for key, val in kwargs.items():
            if not isinstance(val, (int, float)):
                raise BadTypeError(f"Values must be int or float for >= check! Got: {type(val)}")
            self._wheres += [self.Where(key, " >= ", val, or_)]
        return self


    def like(self, or_: bool = False, **kwargs):
        for key, val in kwargs.items():
            if not isinstance(val, str):
                raise BadTypeError(f"Values must be strings for like check! Got: {type(val)}")
            self._wheres += [self.Where(key, " LIKE ", val, or_)]
        return self


    def nlike(self, or_: bool = False, **kwargs):
        for key, val in kwargs.items():
            if not isinstance(val, str):
                raise BadTypeError(f"Values must be strings for not like check! Got: {type(val)}")
            self._wheres += [self.Where(key, " NOT LIKE ", val, or_)]
        return self


    def _compile(self, start_text: str, dbconn: sql.Connection, reset: bool = True) -> sql.Cursor:
        stmt = f"{start_text} \"{self._table.fqcn}\" "
        vals = []
        if self._wheres:
            stmt += "WHERE "
            for where in self._wheres:
                stmt += f"{where.colname}{where.operator}?{where.connector}"
                vals += [where.value]
            stmt = stmt[:-len(self._wheres[-1].connector)]

        if self._limit is not None:
            stmt += f"LIMIT {self._limit}"
            if self._offset:
                stmt += f" OFFSET {self._offset}"

        if reset:
            self._wheres = []
            self._limit = None
            self._offset = None
        return dbconn.execute(stmt + ";", vals)


class Delete(_Query):
    def commit(self, full_count: bool = False) -> int:
        if not self._table.is_parent:
            raise ParentError("Cannot remove non-parent types directly!")
        self.eq(_parent_ = None)

        return self._commit(full_count)


    def _commit(self, count: bool) -> int:
        if not self._table.dbconn:
            raise DBConnError(f"Table {self._table.name} has no valid connection to database!")

        res: list[sql.Row] = self._compile("SELECT * FROM", self._table.dbconn, False).fetchall()
        rlen = len(res)
        self._compile("DELETE FROM", self._table.dbconn)
        self._table.dbconn.commit()

        for key, type_ in self._table.members.items():
            if isinstance(type_, GenericAlias | UnionType):
                type_ = Assembler.get_base_type(type_) # noqa: PLW2901

            if type_ in BASE_TYPES:
                continue

            for item in res:
                if str(item[key])[:2] == "b'" or str(item[key])[:2] == "b\"":
                    continue
                subtype: type = locate(item[key]) # type: ignore
                if subtype not in self._tables:
                    raise BadTypeError("Subtype was invalid!")

                delete = Delete(subtype, self._tables)
                delete.eq(_parent_=item["_uid_"])
                if count:
                    rlen += delete._commit(True)
                else:
                    delete._commit(False)
        return rlen


class Select(_Query):
    def limit(self, limit: int, offset: int | None = None):
        if limit <= 0:
            raise ValueError("Limit must be >= 0!")
        if offset and offset < 0:
            raise ValueError("Offset must be > 0!")

        self._limit = limit
        self._offset = offset
        return self


    def one(self) -> Any:
        if self._limit:
            self._limit = 2
            self._offset = None

        rows = self._compile().fetchall()
        if len(rows) > 1:
            raise QueryError("Too many results found for query")
        if len(rows) == 0:
            raise QueryError("No results found for query")

        return Assembler.assemble_type(self._table.base_type, self._tables, rows[0])


    def first(self) -> Any:
        if self._limit:
            self._limit = 1
            self._offset = None

        return Assembler.assemble_type(
            self._table.base_type, self._tables, self._compile().fetchone()
        )


    def all(self) -> list[Any]:
        rows = self._compile().fetchall()
        return Assembler.assemble_types(self._table.base_type, self._tables, rows)


    def count(self) -> int:
        self.gt(True, _expires_ = time())
        self.eq(_expires_ = None)
        return self._compile("COUNT(*)").fetchone()[0]


    def _compile(self, get_what: str = "*") -> sql.Cursor:
        if not self._table.dbconn:
            raise DBConnError("Table does not have a valid database connection")
        self._table.dbconn.execute(f"DELETE FROM \"{self._table.fqcn}\" WHERE _expires_ < {time()}")
        self._table.dbconn.commit()

        return super()._compile(f"SELECT {get_what} FROM", self._table.dbconn)
