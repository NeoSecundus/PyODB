"""The main module which processes python primitives (and lists and dicts) and generates sql
statements.
Including create and remove table statements in case the fields contained by a class were changed.
"""
import sqlite3 as sql
from pathlib import Path
from threading import get_ident as get_thread_id
from types import UnionType

from pyodb.schema.base._type_defs import BASE_TYPE_SQL_MAP, BASE_TYPES


class Table:
    """A class representing a table in a database, used to store objects of a specific type.

    Args:
        base_type (type): The type of objects that the table will store.
        sharded (bool): A flag indicating whether the table has it's own db file or not.
    """
    base_type: type
    is_parent: bool
    _sharded: bool


    def __init__(
            self,
            base_type: type,
            base_path: Path,
            members: dict[str, type | UnionType],
            sharded: bool
        ) -> None:
        self._members = {}
        self.base_type = base_type
        self.is_parent = False
        self._sharded = sharded
        self.base_path = base_path
        self._members = members
        self._dbconn = self._create_dbconn()
        self._cur_thread = get_thread_id()


    @property
    def members(self) -> dict[str, type | UnionType]:
        """A dictionary containing the members (fields) of the table with their
        corresponding data types."""

        return self._members.copy()


    @property
    def name(self) -> str:
        """The name of the table."""
        return self.base_type.__name__


    @property
    def fqcn(self) -> str:
        """Fully qualified class name"""
        return f"{self.base_type.__module__}.{self.base_type.__name__}"


    @property
    def dbconn(self) -> sql.Connection:
        """SQLite3 Database Connection"""
        tid = get_thread_id()
        if self._cur_thread != tid:
            self._dbconn = self._create_dbconn()
            self._cur_thread = tid

        return self._dbconn


    def create_table(self):
        """
        Creates a new table in the database.

        Raises:
            DBConnError: If the table does not have a valid connection to any database.
        """
        self.dbconn.execute(self._create_table_sql())
        self.dbconn.commit()


    def drop_table(self):
        """
        Deletes the table from the database.

        Raises:
            DBConnError: If the table does not have a valid connection to any database.
        """
        self.dbconn.execute(self._drop_table_sql())
        self.dbconn.commit()


    def delete_parent_entries(self, parent):
        """
        Deletes all rows in the table that have a parent of the specified parent table.

        Args:
            parent (Table): The parent table whose rows should be deleted.

        Raises:
            DBConnError: If the table does not have a valid connection to a database.
        """
        self.dbconn.execute(f"DELETE FROM \"{self.fqcn}\" WHERE _parent_table_ = '{parent.name}'")
        self.dbconn.commit()


    def _create_table_sql(self) -> str:
        """Returns the SQL statement needed to create the table."""
        sql = f"CREATE TABLE IF NOT EXISTS \"{self.fqcn}\" (_uid_ TEXT PRIMARY KEY,_parent_ TEXT,\
_parent_table_ TEXT,_expires_ REAL,"
        for name, type_ in self.members.items():
            if type_ in BASE_TYPES:
                sql += f"{name} {BASE_TYPE_SQL_MAP[type_]},"
            else:
                sql += f"{name} TEXT,"

        return sql[:-1] + ");"


    def _drop_table_sql(self) -> str:
        """Returns the drop table sql for this table."""
        return f"DROP TABLE IF EXISTS \"{self.fqcn}\";"


    def _create_dbconn(self) -> sql.Connection:
        """Static method for creating a new database connection with standard performance boosting
            pragmas.

        Args:
            path (Path): The path to the database file.

        Returns:
            Connection: A new connection object.
        """
        conn = sql.connect(
            self.base_path / (self.base_type.__name__ + ".db")
                if self._sharded
                else self.base_path / "pyodb.db",
            check_same_thread=True,
            isolation_level="IMMEDIATE"
        )
        try:
            # conn.execute("pragma journal_mode = WAL;")
            conn.execute("pragma synchronous = normal;")
            conn.execute("pragma page_size = 4096;")
            conn.commit()
        except sql.OperationalError:
            # These pragmas are only for performance
            # They may fail because the database is locked or because they are not supported
            # it is not critical in any case
            print("PyODB WARNING: Could not set database performance pragmas.")

        conn.row_factory = sql.Row
        return conn


    def __repr__(self) -> str:
        return f"{self.base_type.__name__}: \
{ {k: str(t) if isinstance(t, UnionType) else t.__name__ for k, t in self._members.items()} };"
