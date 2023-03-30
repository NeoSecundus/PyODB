from types import UnionType

BASE_TYPE_SQL_MAP: dict[type | UnionType, str] = {
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
CONTAINERS: list[type] = [
    list, set, frozenset,
    tuple, dict
]
PRIMITIVES: list[type] = [
    int, float, complex,
    bool, str, bytes,
    bytearray
]
BASE_TYPES: list[type | UnionType] = list(BASE_TYPE_SQL_MAP.keys())
