import pickle
import sqlite3.dbapi2 as sql
from types import UnionType

from src.pyodb._util import generate_uid

BASE_TYPE_MAPPINGS: dict[type | UnionType, str] = {
    int: "INTEGER NOT NULL",
    float: "REAL NOT NULL",
    complex: "TEXT NOT NULL",
    bool: "INTEGER NOT NULL",
    str: "TEXT NOT NULL",
    set: "BLOB NOT NULL",
    frozenset: "BLOB NOT NULL",
    dict: "BLOB NOT NULL",
    list: "BLOB NOT NULL",
    tuple: "BLOB NOT NULL",
    bytes: "BLOB NOT NULL",
    bytearray: "BLOB NOT NULL",
    int | None: "INTEGER",
    float | None: "REAL",
    complex | None: "TEXT",
    bool | None: "INTEGER",
    str | None: "TEXT",
    set | None: "BLOB",
    frozenset | None: "BLOB",
    dict | None: "BLOB",
    list | None: "BLOB",
    tuple | None: "BLOB",
    bytes | None: "BLOB",
    bytearray | None: "BLOB",
}
CONTAINERS = [list, set, frozenset, tuple, dict]
PRIMITIVES = [int, float, complex, bool, str, bytes, bytearray]
BASE_TYPES = list(BASE_TYPE_MAPPINGS.keys())


class Insert:
    def __init__(
            self,
            table_name: str,
            parent: str | None,
            parent_table: str | None
        ) -> None:
        self._table_name = table_name
        self._uid = generate_uid()
        self._vals = [parent, parent_table]


    def add_val(self, val: object):
        type_ = type(val)
        if type_ in CONTAINERS:
            self._vals += [pickle.dumps(val)]
        elif type_ in BASE_TYPES or val is None:
            self._vals += [val]
        else:
            self._vals += [f"{type_.__module__}.{type_.__name__}"]


    def execute(self, dbconn: sql.Connection):
        insert = f"INSERT INTO {self._table_name} VALUES("
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


    def execute(self, dbconn: sql.Connection):
        insert = f"INSERT INTO {self._table_name} VALUES("
        insert += "?,"*len(self._vals[0])
        insert = insert[:-1] + ");"
        dbconn.executemany(insert, self._vals)
        dbconn.commit()


    @property
    def vals(self) -> list[tuple]:
        return self._vals


class Update:
    pass


class Delete:
    pass


class Select:
    pass
