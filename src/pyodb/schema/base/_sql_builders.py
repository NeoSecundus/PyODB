import pickle
import sqlite3.dbapi2 as sql
from enum import Enum
from time import time
from types import NoneType
from typing import Any

from src.pyodb._util import generate_uid
from src.pyodb.schema.base._operators import Assembler
from src.pyodb.schema.base._table import Table
from src.pyodb.schema.base._type_defs import CONTAINERS, PRIMITIVES


class Insert:
    def __init__(
            self,
            table_name: str,
            parent: str | None,
            parent_table: str | None,
            expires: int | None
        ) -> None:
        self._table_name = table_name
        self._uid = generate_uid()
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
            raise TypeError(f"Cannot add {type(other)} to MultiInsert")
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
        class CONNECTOR(Enum):
            AND = " AND "
            OR = " OR "

        def __init__(
                self,
                colname: str,
                operator: str,
                value: int|float|str|bool|None,
                connector: CONNECTOR = CONNECTOR.AND
            ) -> None:
            self.colname = colname
            self.operator = operator
            self.value = value
            self.connector = connector


    _table_name: str
    _wheres: list[Where]
    _limit: int | None
    _offset: int | None
    def __init__(self, table_name: str) -> None:
        self._table_name = table_name
        self._wheres = []
        self._limit = None


    def eq(self, **kwargs):
        for key, val in kwargs.items():
            if not isinstance(val, (int, float, str, bool, NoneType)):
                raise TypeError("Values must be int, float, str or bool for == check!")
            if val is None:
                self._wheres += [self.Where(key, " IS ", None)]
            else:
                self._wheres += [self.Where(key, " = ", val)]
        return self


    def ne(self, **kwargs):
        for key, val in kwargs.items():
            if not isinstance(val, (int, float, str, bool, NoneType)):
                raise TypeError("Values must be int, float, str or bool for == check!")
            if val is None:
                self._wheres += [self.Where(key, " IS NOT ", None)]
            else:
                self._wheres += [self.Where(key, " != ", val)]
        return self


    def lt(self, **kwargs):
        for key, val in kwargs.items():
            if not isinstance(val, (int, float)):
                raise TypeError("Values must be int or float for < check!")
            self._wheres += [self.Where(key, " < ", val)]
        return self


    def gt(self, **kwargs):
        for key, val in kwargs.items():
            if not isinstance(val, (int, float)):
                raise TypeError("Values must be int or float for > check!")
            self._wheres += [self.Where(key, " > ", val)]
        return self


    def le(self, **kwargs):
        for key, val in kwargs.items():
            if not isinstance(val, (int, float)):
                raise TypeError("Values must be int or float for <= check!")
            self._wheres += [self.Where(key, " <= ", val)]
        return self


    def ge(self, **kwargs):
        for key, val in kwargs.items():
            if not isinstance(val, (int, float)):
                raise TypeError("Values must be int or float for >= check!")
            self._wheres += [self.Where(key, " >= ", val)]
        return self


    def like(self, **kwargs):
        for key, val in kwargs.items():
            if not isinstance(val, str):
                raise TypeError("Values must be strings for like check!")
            self._wheres += [self.Where(key, " LIKE ", val)]
        return self


    def nlike(self, **kwargs):
        for key, val in kwargs.items():
            if not isinstance(val, str):
                raise TypeError("Values must be strings for not like check!")
            self._wheres += [self.Where(key, " NOT LIKE ", val)]
        return self


    def _compile(self, start_text: str, dbconn: sql.Connection) -> sql.Cursor:
        text = f"{start_text} \"{self._table_name}\" "
        vals = []
        if self._wheres:
            text += "WHERE "
            for where in self._wheres:
                text += f"{where.colname}{where.operator}?{where.connector.value}"
                vals += [where.value]
            text = text[:-len(where.connector.value)]

        if self._limit:
            text += f"LIMIT {self._limit}"
            if self._offset:
                text += f" OFFSET {self._offset}"

        self._wheres = []
        self._limit = None
        self._offset = None
        return dbconn.execute(text + ";", vals)


class Delete(_Query):
    def delete_count(self, dbconn: sql.Connection) -> int:
        before: int = dbconn.execute(f"SELECT COUNT(*) FROM {self._table_name};").fetchone()[0]

        self._compile("DELETE FROM", dbconn)
        dbconn.commit()

        return dbconn.execute(f"SELECT COUNT(*) FROM {self._table_name};").fetchone()[0] - before


    def commit(self, dbconn: sql.Connection):
        self._compile("DELETE FROM", dbconn)
        dbconn.commit()


class Select(_Query):
    def __init__(self, type_: type, tables: dict[type, Table]) -> None:
        self._tables = tables
        self._table = tables[type_]
        super().__init__(tables[type_].fqcn)


    def limit(self, limit: int, offset: int | None = None):
        if limit <= 0:
            raise ValueError("Limit must be >= 0!")
        if offset and offset < 0:
            raise ValueError("Offset must be > 0!")

        self._limit = limit
        self._offset = offset


    def _compile(self) -> sql.Cursor:
        if not self._table.dbconn:
            raise ConnectionError("Table does not have a valid database connection")

        self._table.dbconn.execute(
            f"DELETE FROM \"{self._table_name}\" WHERE _expires_ < {int(time())};"
        )
        self._table.dbconn.commit()
        return super()._compile("SELECT * FROM", self._table.dbconn)


    def one(self) -> Any:
        if self._limit:
            self._limit = 2
            self._offset = None

        rows = self._compile().fetchall()
        if len(rows) > 1:
            raise IndexError("Too many results found for query")
        if len(rows) == 0:
            raise IndexError("No results found for query")

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
        return [Assembler.assemble_type(self._table.base_type, self._tables, row) for row in rows]
