import pickle
import sqlite3.dbapi2 as sql
from pydoc import locate
from time import time
from types import GenericAlias, UnionType
from typing import Any

from pyodb._util import generate_uid
from pyodb.error import BadTypeError, ExpiryError, ParentError, QueryError
from pyodb.schema.base._operators import Assembler
from pyodb.schema.base._table import Table
from pyodb.schema.base._type_defs import BASE_TYPES, CONTAINERS, PRIMITIVES


class Insert:
    """A class for constructing and committing SQL INSERT statements to a database.

    Args:
        table_name (str): The name of the table to insert data into.
        parent (str, optional): A string representing uid of the parent of the inserted data.
            Default is None.
        parent_table (str, optional): A string representing the name of the parent table. Default is
            None.
        expires (float, optional): A floating-point number representing the expiration time of the
            inserted data as a Unix timestamp. Default is None.
    """

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
            raise ExpiryError("expires must be greater than the current timestamp")
        self._vals: list = [parent, parent_table, expires]

    def add_val(self, val: object) -> None:
        """Add a value to the list of values to be inserted into the table.

        Args:
            val (object): The value to be added to the list.
        """
        type_ = type(val)
        if type_ in PRIMITIVES or val is None:
            self._vals += [val]
        elif type_ in CONTAINERS:
            self._vals += [pickle.dumps(val)]
        else:
            self._vals += [f"{type_.__module__}.{type_.__name__}"]

    def commit(self, dbconn: sql.Connection) -> None:
        """Execute the INSERT statement and commit changes to the database.

        Args:
            dbconn (sql.Connection): A connection object to the database.

        """
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
    """
    A class for batching multiple insertions into a single SQL query.

    Args:
        table_name (str): The name of the table to insert values into
    """
    def __init__(self, table_name: str) -> None:
        self._vals: list[tuple] = []
        self._table_name = table_name


    def __add__(self, other: object):
        """
        Adds an Insert or MultiInsert object to the batch.

        Args:
            other (object): The Insert or MultiInsert object to add.

        Returns:
            MultiInsert: The updated MultiInsert object.

        Raises:
            BadTypeError: If other is not an Insert or MultiInsert object.
        """
        if isinstance(other, Insert):
            self._vals += [tuple(other.vals)]
        elif isinstance(other, MultiInsert):
            self._vals += other._vals
        else:
            raise BadTypeError(f"Cannot add {type(other)} to MultiInsert")
        return self


    def commit(self, dbconn: sql.Connection) -> None:
        """
        Commits the batched inserts to the SQL database.

        Args:
            dbconn (sql.Connection): A connection to the SQL database.
        """
        insert = f"INSERT INTO \"{self._table_name}\" VALUES("
        insert += "?,"*len(self._vals[0])
        insert = insert[:-1] + ");"
        with dbconn as conn:
            conn.executemany(insert, self._vals)
            conn.commit()


    @property
    def vals(self) -> list[tuple]:
        return self._vals


class _Query:
    """
    A helper class for building SQL SELECT statements.

    Args:
        type_ (type): The type of the table to query.
        tables (dict[type, Table]): A dictionary of all known tables.
    """
    class Where:
        """
        A helper class for building the WHERE clause of the SQL statement.

        Args:
            colname (str): The name of the column to apply the constraint on.
            operator (str): The comparison operator to use.
            value (int | float | str | bool | None): The value to compare with.
            or_ (bool): A flag indicating whether this constraint should be joined by OR instead of
                AND.
        """
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
        """
        Adds an equality filter to the query.

        Parameters:
            or_ (bool, optional): Whether to use the OR operator in the WHERE clause instead of AND.
                Default is False.
            **kwargs: A dictionary of column names and values to filter on.
                Allowed Types are: int, float, str, bool, NoneType

        Returns:
            self: The _Query instance.

        Raises:
            BadTypeError: If the passed argument has an invalid type.
        """
        for key, val in kwargs.items():
            if val and not isinstance(val, (int, float, str, bool)):
                raise BadTypeError(
                    f"Values must be int, float, str or bool for == check! Got: {type(val)}"
                )
            if val is None:
                self._wheres += [self.Where(key, " IS ", None, or_)]
            else:
                self._wheres += [self.Where(key, " = ", val, or_)]
        return self


    def ne(self, or_: bool = False, **kwargs):
        """
        Adds a non-equality filter to the query.

        Parameters:
            or_ (bool, optional): Whether to use the OR operator in the WHERE clause instead of AND.
                Default is False.
            **kwargs: A dictionary of column names and values to filter on.
                Allowed Types are: int, float, str, bool, NoneType

        Returns:
            self: The _Query instance.

        Raises:
            BadTypeError: If the passed argument has an invalid type.
        """
        for key, val in kwargs.items():
            if val and not isinstance(val, (int, float, str, bool)):
                raise BadTypeError(
                    f"Values must be int, float, str or bool for != check! Got: {type(val)}"
                )
            if val is None:
                self._wheres += [self.Where(key, " IS NOT ", None, or_)]
            else:
                self._wheres += [self.Where(key, " != ", val, or_)]
        return self


    def lt(self, or_: bool = False, **kwargs):
        """
        Adds a les-than filter to the query.

        Parameters:
            or_ (bool, optional): Whether to use the OR operator in the WHERE clause instead of AND.
                Default is False.
            **kwargs: A dictionary of column names and values to filter on.
                Allowed Types are: int, float

        Returns:
            self: The _Query instance.

        Raises:
            BadTypeError: If the passed argument has an invalid type.
        """
        for key, val in kwargs.items():
            if not isinstance(val, (int, float)):
                raise BadTypeError(f"Values must be int or float for < check! Got: {type(val)}")
            self._wheres += [self.Where(key, " < ", val, or_)]
        return self


    def gt(self, or_: bool = False, **kwargs):
        """
        Adds a greater-than filter to the query.

        Parameters:
            or_ (bool, optional): Whether to use the OR operator in the WHERE clause instead of AND.
                Default is False.
            **kwargs: A dictionary of column names and values to filter on.
                Allowed Types are: int, float

        Returns:
            self: The _Query instance.

        Raises:
            BadTypeError: If the passed argument has an invalid type.
        """
        for key, val in kwargs.items():
            if not isinstance(val, (int, float)):
                raise BadTypeError(f"Values must be int or float for > check! Got: {type(val)}")
            self._wheres += [self.Where(key, " > ", val, or_)]
        return self


    def le(self, or_: bool = False, **kwargs):
        """
        Adds a less-than-or-equal filter to the query.

        Parameters:
            or_ (bool, optional): Whether to use the OR operator in the WHERE clause instead of AND.
                Default is False.
            **kwargs: A dictionary of column names and values to filter on.
                Allowed Types are: int, float

        Returns:
            self: The _Query instance.

        Raises:
            BadTypeError: If the passed argument has an invalid type.
        """
        for key, val in kwargs.items():
            if not isinstance(val, (int, float)):
                raise BadTypeError(f"Values must be int or float for <= check! Got: {type(val)}")
            self._wheres += [self.Where(key, " <= ", val, or_)]
        return self


    def ge(self, or_: bool = False, **kwargs):
        """
        Adds a greater-than-or-equal filter to the query.

        Parameters:
            or_ (bool, optional): Whether to use the OR operator in the WHERE clause instead of AND.
                Default is False.
            **kwargs: A dictionary of column names and values to filter on.
                Allowed Types are: int, float

        Returns:
            self: The _Query instance.

        Raises:
            BadTypeError: If the passed argument has an invalid type.
        """
        for key, val in kwargs.items():
            if not isinstance(val, (int, float)):
                raise BadTypeError(f"Values must be int or float for >= check! Got: {type(val)}")
            self._wheres += [self.Where(key, " >= ", val, or_)]
        return self


    def like(self, or_: bool = False, **kwargs):
        """
        Adds a "like" filter to the query.

        Parameters:
            or_ (bool, optional): Whether to use the OR operator in the WHERE clause instead of AND.
                Default is False.
            **kwargs: A dictionary of column names and values to filter on.
                Allowed Types are: str

        Returns:
            self: The _Query instance.

        Raises:
            BadTypeError: If the passed argument has an invalid type.
        """
        for key, val in kwargs.items():
            if not isinstance(val, str):
                raise BadTypeError(f"Values must be strings for like check! Got: {type(val)}")
            self._wheres += [self.Where(key, " LIKE ", val, or_)]
        return self


    def nlike(self, or_: bool = False, **kwargs):
        """
        Adds a "not like" filter to the query.

        Parameters:
            or_ (bool, optional): Whether to use the OR operator in the WHERE clause instead of AND.
                Default is False.
            **kwargs: A dictionary of column names and values to filter on.
                Allowed Types are: str

        Returns:
            self: The _Query instance.

        Raises:
            BadTypeError: If the passed argument has an invalid type.
        """
        for key, val in kwargs.items():
            if not isinstance(val, str):
                raise BadTypeError(f"Values must be strings for not like check! Got: {type(val)}")
            self._wheres += [self.Where(key, " NOT LIKE ", val, or_)]
        return self


    def _compile(self, start_text: str, dbconn: sql.Connection, reset: bool = True) -> sql.Cursor:
        """
        Compiles and executes the SQL query, returning a cursor to the result set.

        Args:
            start_text (str): The initial text of the SQL query, such as "SELECT * FROM".
            dbconn (sqlite3.Connection): A connection object to the database.
            reset (bool): A flag indicating whether to reset the query after execution. Default is
                True.

        Returns:
            sqlite3.Cursor: A cursor to the result set.

        Raises:
            sqlite3.Error: If there is a problem executing the query.
        """
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
    """
    Class representing a DELETE SQL statement to delete records from a table.
    Inherits from `_Query` class.
    """
    def commit(self, full_count: bool = False) -> int:
        """
        Deletes records from the table and returns the number of records deleted.

        Parameters:
        full_count (bool): Whether to also count the deleted children or not. Defaults to False.

        Returns:
            int: The number of records deleted.
        """
        if not self._table.is_parent:
            raise ParentError("Cannot remove non-parent types directly!")
        self.eq(_parent_ = None)

        return self._commit(full_count)


    def _commit(self, count: bool) -> int:
        """Commits the DELETE statement to the database and returns the number of records deleted.
            This method is used recursively to delete records from all child tables that have a
            foreign key constraint with the parent table being deleted.

            Parameters:
                count (bool): Whether to count the deleted children or not. Defaults to False.

            Returns:
                int: The number of records deleted.
        """
        res: list[sql.Row] = self._compile("SELECT * FROM", self._table.dbconn, False).fetchall()
        rlen = len(res)
        self._compile("DELETE FROM", self._table.dbconn)
        self._table.dbconn.commit()

        for key, type_ in self._table.members.items():
            if isinstance(type_, GenericAlias | UnionType):
                type_ = Assembler.get_base_type(type_)

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
    """A class representing a SELECT query to retrieve data from a database table.
    Inherits from the _Query class.
    """
    def limit(self, limit: int, offset: int | None = None):
        if limit <= 0:
            raise ValueError("Limit must be >= 0!")
        if offset and offset < 0:
            raise ValueError("Offset must be > 0!")

        self._limit = limit
        self._offset = offset
        return self


    def one(self) -> Any:
        """
        Select EXACTLY one result of the query and return it as an object of the table's base type.

        Returns:
            Any: An object of the base type of the table representing the result.

        Raises:
            QueryError: If no results or more than one result is found.
        """
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
        """
        Returns the first result of the query as an object of the base type of the table.

        Returns:
            Any: An object of the base type of the table representing the first result or None if
                no table was found.
        """
        if self._limit:
            self._limit = 1

        row = self._compile().fetchone()
        if row is None:
            return None

        return Assembler.assemble_type(
            self._table.base_type, self._tables, row
        )


    def all(self) -> list[Any]:
        """
        Returns a list of all results of the query as objects of the base type of the table.

        Returns:
            list[Any]: A list of objects of the base type of the table representing all results.
        """
        rows = self._compile().fetchall()
        return Assembler.assemble_types(self._table.base_type, self._tables, rows)


    def count(self) -> int:
        """
        Returns the number of rows in the table matching the query. Alos omits expired entries.

        Returns:
            int: The number of rows matching the query.
        """
        self.gt(True, _expires_ = time())
        self.eq(_expires_ = None)
        return self._compile("COUNT(*)").fetchone()[0]


    def _compile(self, get_what: str = "*") -> sql.Cursor: # type: ignore
        """
        Compiles and executes the SELECT query and returns a cursor object.

        Args:
            get_what (str, optional): The columns to select in the query. Defaults to "*".

        Returns:
            sql.Cursor: A cursor object representing the results of the query.

        Raises:
            DBConnError: If the table does not have a valid database connection.
        """
        self._table.dbconn.execute(f"DELETE FROM \"{self._table.fqcn}\" WHERE _expires_ < {time()}")
        self._table.dbconn.commit()

        return super()._compile(f"SELECT {get_what} FROM", self._table.dbconn)
