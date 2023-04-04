import pickle
import sqlite3 as sql
from pydoc import locate
from time import time
from types import GenericAlias, NoneType, UnionType
from typing import Any

from src.pyodb.error import DBConnError, DisassemblyError, MixedTypesError
from src.pyodb.schema.base._table import Table
from src.pyodb.schema.base._type_defs import BASE_TYPES, CONTAINERS, PRIMITIVES


class Assembler:
    last_clean = 0
    @classmethod
    def _get_sub_rows(
            cls,
            table: Table,
            tables: dict[type, Table],
            parent: str
        ) -> dict[str, object]:
        if not table.dbconn:
            raise DBConnError(
                f"Table '{table.name}' has no valid database connection!"
            )
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
        if isinstance(type_, UnionType):
            args = type_.__args__
            subtype = args[0] if not isinstance(args[0], NoneType) else args[1]

            if isinstance(subtype, GenericAlias):
                return subtype.__origin__

            return subtype
        else:
            return type_.__origin__


    @classmethod
    def assemble_types(
            cls,
            base_type: type,
            tables: dict[type, Table],
            rows: list[sql.Row]
        ) -> list[Any]:
        table = tables[base_type]
        objs = []
        subrows: dict[type, dict[str, object]] = {}
        for row in rows:
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
                    if str(row[name])[:2] == "b'" or str(row[name])[:2] == "b\"":
                        setattr(obj, name, pickle.loads(row[name]))
                        continue

                    ttype: type = locate(row[name]) # type: ignore

                    if ttype not in subrows:
                        subrows[ttype] = cls._get_sub_rows(tables[ttype], tables, table.fqcn)
                    try:
                        setattr(obj, name, subrows[ttype][row["_uid_"]])
                    except KeyError:
                        setattr(obj, name, None)
            if "__odb_reassemble__" in base_type.__dict__:
                obj.__odb_reassemble__()
            objs += [obj]
        return objs


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
                if str(row[name])[:2] == "b'" or str(row[name])[:2] == "b\"":
                    setattr(obj, name, pickle.loads(row[name]))
                    continue

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
    @classmethod
    def _disassemble_union_type(cls, type_: UnionType) -> list[Table]:
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
                tables += cls.disassemble_type(t)
        return tables


    @classmethod
    def disassemble_type(cls, obj_type: type) -> list[Table]:
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
                    tables += cls._disassemble_union_type(type_)
                else:
                    tables += cls.disassemble_type(type_)

        return tables
