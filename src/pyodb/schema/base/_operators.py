import pickle
import sqlite3 as sql
from pydoc import locate
from time import time
from types import GenericAlias, NoneType, UnionType
from typing import Any, Callable, Coroutine, Generator

from pyodb.error import DisassemblyError, MixedTypesError
from pyodb.schema.base._table import Table
from pyodb.schema.base._type_defs import BASE_TYPES, CONTAINERS, PRIMITIVES


class Assembler:
    last_clean: float = 0
    @classmethod
    def _get_sub_rows(
            cls,
            table: Table,
            tables: dict[type, Table],
            parent: str
        ) -> dict[str, object]:
        """
        Get the rows for the subtypes of the table with the given parent id.

        Args:
            table (Table): The table from which to retrieve the rows.
            tables (dict[type, Table]): Dictionary mapping the types to their corresponding tables.
            parent (str): The parent id used to retrieve the rows.

        Returns:
            dict[str, object]: A dictionary containing the retrieved rows.

        Raises:
            DBConnError: If the table does not have a valid database connection.
        """
        if cls.last_clean < time()-1:
            table.dbconn.execute(f"DELETE FROM \"{table.fqcn}\" WHERE _expires_ < {time()}")
            table.dbconn.commit()
            cls.last_clean = time()
        rows = table.dbconn.execute(
            f"SELECT * FROM \"{table.fqcn}\" WHERE _parent_table_ = ? ORDER BY _parent_ DESC",
            [parent]
        ).fetchall()
        objs = cls.assemble_types(table.base_type, tables, rows)
        return {rows[i]["_parent_"]: objs[i] for i in range(len(rows))}


    @classmethod
    def get_base_type(cls, type_: UnionType | GenericAlias) -> type:
        """
        Given a UnionType, this method returns the most basic type.

        Args:
            type_ (UnionType): A UnionType.

        Returns:
            type: The base type of the given type.
        """
        args = type_.__args__
        subtype = args[0] if not isinstance(args[0], NoneType) else args[1]

        return subtype


    @classmethod
    def assemble_types(
            cls,
            base_type: type,
            tables: dict[type, Table],
            rows: list[sql.Row]
        ) -> list[Any]:
        """
        Assemble multiple objects of the given type from SQL rows.

        Args:
            base_type (type): The type of the object to assemble.
            tables (dict[type, Table]): A dictionary of type -> Table mappings.
            rows (list[sql.Row]): The SQL rows to assemble the objects from.

        Returns:
            Any: An instance of the given type, populated with values from the SQL row.
        """
        table = tables[base_type]
        objs = []
        subrows: dict[type, dict[str, object]] = {}
        for row in rows:
            obj: Any = object.__new__(base_type)
            for name, type_ in table.members.items():
                if row[name] is None:
                    obj.__dict__[name] = None
                    continue

                if isinstance(type_, (GenericAlias, UnionType)):
                    type_ = cls.get_base_type(type_)

                if type_ in PRIMITIVES:
                    obj.__dict__[name] = type_(row[name])

                elif type_ in CONTAINERS:
                    obj.__dict__[name] = pickle.loads(row[name])

                elif isinstance(type_, type):
                    if str(row[name])[:2] == "b'" or str(row[name])[:2] == "b\"":
                        obj.__dict__[name] = pickle.loads(row[name])
                        continue

                    ttype: type = locate(row[name]) # type: ignore

                    if ttype not in subrows:
                        subrows[ttype] = cls._get_sub_rows(tables[ttype], tables, table.fqcn)
                    obj.__dict__[name] = subrows[ttype][row["_uid_"]]
            if "__odb_reassemble__" in base_type.__dict__:
                obj.__odb_reassemble__()
            objs += [obj]
        return objs


    @classmethod
    def assemble_type(cls, base_type: type, tables: dict[type, Table], row: sql.Row) -> Any:
        """
        Assemble a single object of the given type from a single SQL row.

        Args:
            base_type (type): The type of the object to assemble.
            tables (dict[type, Table]): A dictionary of type -> Table mappings.
            row (sql.Row): The SQL row to assemble the object from.

        Returns:
            Any: An instance of the given type, populated with values from the SQL row.

        Raises:
            DBConnError: In case a sub-table does not have a valid database connection.
        """
        table = tables[base_type]
        obj: Any = object.__new__(base_type)
        for name, type_ in table.members.items():
            if row[name] is None:
                obj.__dict__[name] = None
                continue

            if isinstance(type_, (GenericAlias, UnionType)):
                type_ = cls.get_base_type(type_)

            if type_ in PRIMITIVES:
                obj.__dict__[name] = type_(row[name])

            elif type_ in CONTAINERS:
                obj.__dict__[name] = pickle.loads(row[name])

            elif isinstance(type_, type):
                if str(row[name])[:2] == "b'" or str(row[name])[:2] == "b\"":
                    obj.__dict__[name] = pickle.loads(row[name])
                    continue

                ttype: type = locate(row[name]) # type: ignore
                subtable = tables[ttype]
                subrow: sql.Row = subtable.dbconn.execute(
                    f"SELECT * FROM \"{subtable.fqcn}\" WHERE _parent_ = '{row['_uid_']}'"
                ).fetchone()
                obj.__dict__[name] = cls.assemble_type(ttype, tables, subrow)
        if "__odb_reassemble__" in base_type.__dict__:
            obj.__odb_reassemble__()
        return obj


class Disassembler:
    sharded = False

    @classmethod
    def _disassemble_union_type(cls, type_: UnionType) -> dict[type, dict]:
        """
        Given a UnionType object, returns a list of Table objects associated with the input type.

        Args:
            type_: A UnionType object.

        Returns:
            A list of Table objects associated with each type of the Union.

        Raises:
            MixedTypesError: If the input type is a UnionType with mixed primitive and custom type
            annotations or multiple primitives.
        """
        tables: dict = {}
        if any([t in BASE_TYPES for t in type_.__args__]):
            raise MixedTypesError(
                f"Cannot save object with mixed primitive and custom type annotations \
or multiple primitives! Got: {type_}"
            )
        for t in type_.__args__:
            if t is NoneType:
                continue
            else:
                tables |= cls.disassemble_type(t)
        return tables


    @classmethod
    def _break_down_type(cls, type_: type | UnionType | GenericAlias) -> type | UnionType:
        if isinstance(type_, UnionType):
            subtypes = type_.__args__
            for i, arg in enumerate(subtypes):
                if isinstance(arg, GenericAlias):
                    if len(subtypes) != 2 or subtypes[1-i] is not NoneType:
                        raise MixedTypesError(
                            "Cannot save object with multiple primitive or mixed types"
                        )
                    type_ = arg.__origin__ | None
                    break

        if hasattr(type_, "__origin__"):
            type_ = type_.__origin__

        if type_ is Any:
            raise MixedTypesError("Type of class member cannot be 'Any'!")

        return type_


    @classmethod
    def disassemble_type(cls, obj_type: type) -> dict[type, dict[str, type | UnionType]]:
        """Disassembles a custom object type into a list of tables that represent the object's
        structure in the database.

        Args:
            obj_type (type): A custom object type to be disassembled.

        Returns:
            list[Table]: A list of Table objects representing the structure of the disassembled
                object type. Each Table object represents a table in the database, and its
                attributes represent the columns in that table.

        Raises:
            DisassemblyError: If obj_type is Any, NoneType, a primitive type or the passed argument
                is not a type at all.
        """
        if obj_type is Any or obj_type is NoneType or obj_type in BASE_TYPES:
            raise DisassemblyError("'Any', 'None' and Primitive types are not supported!")

        if not isinstance(obj_type, type):
            raise DisassemblyError(f"Passed argument must be a type! Got: {obj_type}")

        tables: dict = {obj_type: {}}

        if hasattr(obj_type, "__odb_members__"):
            members: dict[str, type|UnionType|GenericAlias] = getattr(obj_type, "__odb_members__")
        else:
            members = {
                key: type_
                for key, type_
                in obj_type.__annotations__.items()
                if key[:2] != "__" and type_ not in (Callable, Coroutine, Generator)
            }

        for key, type_ in members.items():
            type_ = cls._break_down_type(type_)

            tables[obj_type][key] = type_
            if type_ in BASE_TYPES:
                continue

            if isinstance(type_, UnionType):
                tables |= cls._disassemble_union_type(type_)
            elif isinstance(type_, (type, GenericAlias)) and hasattr(type_, "__annotations__"):
                tables |= cls.disassemble_type(type_)

        return tables
