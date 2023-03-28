"""The main module which processes python primitives (and lists and dicts) and generates sql
statements.
Including create and remove table statements in case the fields contained by a class were changed.
"""
from types import GenericAlias, UnionType

# SQLITE3 Available Data Types:
# TEXT = str, list, dict
# NUMERIC = int, float, date, datetime
#   ^Converts float to int if float does not have decimal places (10.01 => float, 10.0 = int)
# INTEGER = int
# REAL = float
# BLOB = binary or undefined type
PRIMITIVE_MAPPINGS: dict[type | UnionType, str] = {
    int: "INTEGER NOT NULL",
    float: "REAL NOT NULL",
    complex: "TEXT NOT NULL",
    bool: "INTEGER NOT NULL",
    str: "TEXT NOT NULL",
    set: "TEXT NOT NULL",
    frozenset: "TEXT NOT NULL",
    dict: "TEXT NOT NULL",
    list: "TEXT NOT NULL",
    tuple: "TEXT NOT NULL",
    bytes: "BLOB NOT NULL",
    bytearray: "BLOB NOT NULL",
    int | None: "INTEGER",
    float | None: "REAL",
    complex | None: "TEXT",
    bool | None: "INTEGER",
    str | None: "TEXT",
    set | None: "TEXT",
    frozenset | None: "TEXT",
    dict | None: "TEXT",
    list | None: "TEXT",
    tuple | None: "TEXT",
    bytes | None: "BLOB",
    bytearray | None: "BLOB",
}
PRIMITIVES = list(PRIMITIVE_MAPPINGS.keys())

class Table:
    BASE_MEMBERS = {
        "_uid_": str,
        "_members_": dict | None,
        "_parent_": str | None,
        "_pickle_": bytes
    }
    _members: dict[str, type | UnionType | GenericAlias]
    base_type: type
    is_parent: bool


    def __init__(self, base_type: type, is_parent: bool = False) -> None:
        self._members = {}
        self.base_type = base_type
        self.is_parent = is_parent


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
    def base_name(self) -> str:
        return f"{self.base_type.__module__}.{self.base_type.__name__}".replace(".", "_")


    def create_table_sql(self) -> str:
        sql = f"CREATE TABLE {self.base_name} (_uid_ TEXT PRIMARY KEY,_parent_ TEXT,\
_parent_table_ TEXT,_container_ TEXT,"
        for name, type_ in self.members.items():
            if isinstance(type_, (type, UnionType)) and type_ in PRIMITIVES:
                sql += f"{name} {PRIMITIVE_MAPPINGS[type_]},"
            elif isinstance(type_, GenericAlias):
                sql += f"{name} {PRIMITIVE_MAPPINGS[type_.__origin__]},"
            else:
                sql += f"{name} TEXT,"

        return sql[:-1] + ");"


    def drop_table_sql(self) -> str:
        return f"DROP TABLE {self.base_name};"


    def __repr__(self) -> str:
        return f"{self.base_type.__name__}: \
{ {k: str(t) if isinstance(t, UnionType) else t.__name__ for k, t in self._members.items()} }"
