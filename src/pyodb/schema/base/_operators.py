import pickle
import sqlite3 as sql
from pydoc import locate
from types import GenericAlias, NoneType, UnionType
from typing import Any

from src.pyodb.error import DBConnError, DisassemblyError, MixedTypesError
from src.pyodb.schema.base._table import Table
from src.pyodb.schema.base._type_defs import BASE_TYPES, CONTAINERS, PRIMITIVES


class Assembler:
    @classmethod
    def get_base_type(cls, type_: UnionType | GenericAlias) -> type:
        if isinstance(type_, UnionType):
            args = type_.__args__
            subtype = args[0] if not isinstance(args[0], NoneType) else args[1]

            if isinstance(subtype, GenericAlias):
                return subtype.__origin__

            return subtype
        else:
            return type_.__origin__


    @classmethod
    def assemble_type(cls, base_type: type, tables: dict[type, Table], row: sql.Row) -> Any:
        table = tables[base_type]
        obj = object.__new__(base_type)
        for name, type_ in table.members.items():
            if row[name] is None:
                setattr(obj, name, None)
                continue

            if isinstance(type_, (GenericAlias, UnionType)):
                type_ = cls.get_base_type(type_) # noqa: PLW2901

            if type_ in PRIMITIVES:
                setattr(obj, name, type_(row[name]))

            elif type_ in CONTAINERS:
                setattr(obj, name, pickle.loads(row[name]))

            elif isinstance(type_, type):
                ttype: type = locate(row[name]) # type: ignore
                subtable = tables[ttype]
                if not subtable.dbconn:
                    raise DBConnError("Table does not have a valid database connection!")
                subrow: sql.Row = subtable.dbconn.execute(
                    f"SELECT * FROM \"{subtable.fqcn}\" WHERE _parent_ = '{row['_uid_']}'"
                ).fetchone()
                setattr(obj, name, cls.assemble_type(ttype, tables, subrow))
        if "__odb_reassemble__" in base_type.__dict__:
            obj.__odb_reassemble__()
        return obj


class Disassembler:
    max_depth: int


    @classmethod
    def _disassemble_union_type(cls, type_: UnionType, depth: int) -> list[Table]:
        tables = []
        if any([t in BASE_TYPES for t in type_.__args__]):
            raise MixedTypesError(
                f"Cannot save object with mixed primitive and custom type annotations \
or multiple primitives! Got: {type_}"
            )
        for t in type_.__args__:
            if t is NoneType or isinstance(t, GenericAlias):
                continue
            else:
                tables += cls.disassemble_type(t, depth+1)
        return tables


    @classmethod
    def disassemble_type(cls, obj_type: type, depth: int = 0) -> list[Table]:
        if depth > cls.max_depth:
            raise DisassemblyError(f"Surpassed max depth when disassembling type {obj_type}")

        if obj_type is Any or obj_type is NoneType or obj_type in BASE_TYPES:
            raise DisassemblyError("'Any', 'None' and 'Primitive' types are not supported!")

        if not isinstance(obj_type, type):
            raise DisassemblyError("Passed argument must be a type!")

        tables = [Table(obj_type)]

        if "__odb_members__" in obj_type.__annotations__:
            members = getattr(obj_type, "__odb_members__")
        else:
            members: dict[str, type | UnionType | GenericAlias] = {
                key: type_
                for key, type_
                in obj_type.__annotations__.items()
                if key[:2] != "__"
            }

        for key, type_ in members.items():
            tables[0].add_member(key, type_)
            if isinstance(type_, GenericAlias):
                continue

            if type_ not in BASE_TYPES:
                if isinstance(type_, UnionType):
                    tables += cls._disassemble_union_type(type_, depth)
                else:
                    tables += cls.disassemble_type(type_, depth+1)

        return tables
