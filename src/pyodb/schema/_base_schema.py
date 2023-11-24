import pickle
from pathlib import Path
from types import UnionType

from pyodb.error import DisassemblyError, ParentError, UnknownTypeError
from pyodb.schema.base._sql_builders import Delete, Insert, MultiInsert, Select
from pyodb.schema.base._table import Table
from pyodb.schema.base._type_defs import BASE_TYPES


class BaseSchema:
    """Base class for defining database schemas.

    This class defines methods for creating, dropping, and modifying tables to hold objects in a
    database. Objects are added to the schema by their respective types, which shall be defined as
    classes with annotations to indicate the attributes of the object. The class also allows
    querying, updating, and deleting objects in the database.

    Args:
        base_path (Path): The path to the database file.
        max_depth (int): The maximum depth to which nested objects are inserted into the database.
        persistent (bool): If True, the schema instance will be saved to disk upon exit.
    """
    _tables: dict[type, Table]
    _base_path: Path
    _max_depth: int
    is_persistent: bool
    save_table_defs: bool


    def __init__(self, base_path: Path, max_depth: int, persistent: bool) -> None:
        self._tables = {}
        self._max_depth = max_depth
        self._base_path = base_path
        self.is_persistent = persistent
        self.save_table_defs = True


    def is_known_type(self, obj_type: type) -> bool:
        """Check whether the type is already defined in the schema

        Args:
            obj_type (type): Type to check for a schema definition

        Returns:
            bool: True if found, otherwise False
        """
        return obj_type in self._tables


    def add_type(self, base_type: type):
        """Add a new type to the schema.

        This method creates a new table in the database to hold objects of the given type and all
            subtypes.

        Args:
            base_type (type): The type to add to the schema.
        """
        raise NotImplementedError()


    def remove_type(self, base_type: type):
        """Remove a type from the schema.

        This method drops the table associated with the given type from the database as well as all
        child tables. Table cannot be dropped if a parent table depends on it.
        Child tables are ignored if other tables depend on them.

        Args:
            base_type (type): The type to remove from the schema.

        Raises:
            UnknownTypeError: If the given type is not in the schema.
            ParentError: If the given type has a parent table in the schema.
        """
        if not self.is_known_type(base_type):
            raise UnknownTypeError(f"Cannot remove type! Unknown type: {base_type}")
        parent = self.get_parent(base_type)
        if parent:
            raise ParentError(
                f"'{base_type}' could not be removed because '{parent}' depends on it!"
            )
        self._remove_type(None, base_type)


    def _remove_type(self, previous: Table | None, base_type: type):
        """Drops the passed table and all child tables recusively, unless child tables have
        other parent tables.

        Args:
            previous (Table): The parent table of the current table if any.
            base_type (type): The base_type of the table to be removed.
        """
        if previous and (self.get_parent(base_type) or self._tables[base_type].is_parent):
            self._tables[base_type].delete_parent_entries(previous)
            return

        table = self._tables.pop(base_type)
        table.drop_table()
        for _, type_ in table.members.items():
            if isinstance(type_, UnionType):
                types = type_.__args__
                for sub_type in types:
                    if self.is_known_type(sub_type):
                        self._remove_type(table, sub_type)
            elif self.is_known_type(type_):
                self._remove_type(table, type_)


    def get_parent(self, base_type: type) -> type | None:
        """Returns the parent type for the given base_type, if any.

        Args:
            base_type (type): The base type to get the parent for

        Returns:
            type or None: The parent type of the given base_type, or None if no parent is found.
        """
        if not self.is_known_type(base_type):
            raise UnknownTypeError(f"Tried to get parent of unknown type: {base_type}")

        for ttype, table in self._tables.items():
            if ttype == base_type or not table.is_parent:
                continue

            for _, type_ in table.members.items():
                if type_ in BASE_TYPES:
                    continue

                if isinstance(type_, UnionType):
                    if any(base_type == t for t in type_.__args__):
                        return ttype
                    continue
                elif type_ == base_type:
                    return ttype
        return None


    def insert(
            self, obj: object,
            expires: float | None,
            parent: Insert | None = None,
            depth: int = 0
        ):
        """Inserts an object into the database.

        Args:
            obj (object): The object to be inserted.
            expires (float | None): The expiration time of the object.
            parent (Insert | None, optional): The parent object (if the object is nested). Defaults
                to None.
            depth (int, optional): The depth of the object before pickling is used
                (if it is nested). Defaults to 0.

        Raises:
            UnknownTypeError: In case the wanted type is not within the schema

        """
        if not self.is_known_type(type(obj)):
            raise UnknownTypeError(f"Tried to insert object of unknown type {type(obj)}")

        table = self._tables[type(obj)]

        if parent:
            inserter = Insert(table.fqcn, parent.uid, parent.table_name, expires)
        else:
            inserter = Insert(table.fqcn, None, None, expires)

        for key, member_type in table.members.items():
            member = getattr(obj, key)
            if member and member_type not in BASE_TYPES:
                if depth >= self._max_depth:
                    inserter.add_val(pickle.dumps(member))
                    continue
                self.insert(member, expires, inserter, depth+1)
            inserter.add_val(member if isinstance(member, member_type) else member_type(member))
        inserter.commit(table.dbconn)


    def insert_many(self, objs: list, expires: float | None):
        """Inserts a list of objects into the database. Objects must all have the same type.

        Args:
            objs (list): The list of objects to be inserted.
            expires (float | None): The expiration time of the objects.

        Raises:
            UnknownTypeError: In case the type is not within the schema.
            DBConnError: In case a table has no valid database connection
            DisassemblyError: In case the objs within the list are not all of the same type.
        """
        base_type = type(objs[0])
        if not self.is_known_type(base_type):
            raise UnknownTypeError(f"Tried to insert object of unknown type {base_type}")

        table = self._tables[base_type]
        multi_inserter = MultiInsert(table.fqcn)

        if any(type(obj) != base_type for obj in objs):
            raise DisassemblyError("Types in inserted list must all be the same!")

        subtypes: dict[type, list[tuple[object, Insert]]] = {}
        for obj in objs:
            inserter = Insert(table.fqcn, None, None, expires)

            for member in table.members.keys():
                member = getattr(obj, member)
                membertype = type(member)
                if member and membertype not in BASE_TYPES:
                    if self._max_depth == 0:
                        inserter.add_val(pickle.dumps(member))
                        continue

                    if membertype not in subtypes:
                        subtypes[membertype] = []
                    subtypes[membertype] += [(member, inserter)]
                inserter.add_val(member)
            multi_inserter += inserter

        for sub in subtypes.values():
            self._insert_many(sub, expires, 1)
        multi_inserter.commit(table.dbconn)


    def _insert_many(self, objs: list[tuple[object, Insert]], expires: float | None, depth: int):
        """Inserts multiple objects into the database.

        If an object has members that are also objects, this method will recursively insert them as
        well, up to the maximum depth allowed by the schema. Then pickling is used.

        Args:
            objs (list[tuple[object, Insert]]): A list of objects and their corresponding parent
            insert statements.
            expires (float | None): The expiration time for the objects.
            depth (int): The current recursion depth of the object hierarchy.

        Raises:
            DBConnError: If the current table does not have a valid database connection.
        """
        base_type = type(objs[0][0])

        table = self._tables[base_type]
        multi_inserter = MultiInsert(table.fqcn)

        subtypes: dict[type, list[tuple[object, Insert]]] = {}
        for obj in objs:
            inserter = Insert(table.fqcn, obj[1].uid, obj[1].table_name, expires)

            for member in table.members.keys():
                member = getattr(obj[0], member)
                membertype = type(member)
                if member and membertype not in BASE_TYPES:
                    if depth >= self._max_depth:
                        inserter.add_val(pickle.dumps(member))
                        continue

                    if membertype not in subtypes:
                        subtypes[membertype] = []
                    subtypes[membertype] += [(member, inserter)]
                inserter.add_val(member)
            multi_inserter += inserter

        for sub in subtypes.values():
            self._insert_many(sub, expires, depth+1)
        multi_inserter.commit(table.dbconn)


    def select(self, type_: type) -> Select:
        """Returns a `Select` object for the given type. The `Select` object can be used to query
        the database and retrieve objects of the given type.

        Args:
            type_ (type): The type of the object to build the query for.

        Returns:
            Select: A `Select` object for the given type.

        Raises:
            UnknownTypeError: If the given type is not known to the database.
        """
        if not self.is_known_type(type_):
            raise UnknownTypeError(f"Tried to select unknown type: {type_}")
        return Select(type_, self._tables)


    def delete(self, type_: type) -> Delete:
        """Returns a `Delete` object for the given type. The `Delete` object can be used to remove
        objects of the given type from the database.

        Args:
            type_ (type): The type of the object to build the delete for.

        Returns:
            Delete: A `Delete` object for the given type.

        Raises:
            UnknownTypeError: If the given type is not known to the database.
        """
        if not self.is_known_type(type_):
            raise UnknownTypeError(f"Tried to delete instance of unknown type: {type_}")
        return Delete(type_, self._tables)


    def clear(self):
        """Deletes all objects from the database but keeps the table definitions."""
        for table in self._tables.values():
            if not table.is_parent:
                continue
            Delete(table.base_type, self._tables).commit()


    @property
    def max_depth(self) -> int:
        return self._max_depth


    @max_depth.setter
    def max_depth(self, val: int):
        if val < 0:
            raise ValueError("max_depth must be >= 0!")
        self._max_depth = val


    @property
    def schema_size(self) -> int:
        """Number of table definitions / types in the current schema"""
        return len(self._tables)


    def _save_schema(self):
        """Save schema to the database for later re-loads"""
        raise NotImplementedError()
