from types import GenericAlias, NoneType, UnionType
from typing import Any

from src.pyodb.schema.base._table import PRIMITIVES, Table


class DBOperator:
    pass


class Assembler:
    pass


class Disassembler:
    max_depth: int


    @classmethod
    def _disassemble_generic_alias(cls, base_type: GenericAlias, depth: int) -> list[Table]:
        tables = []
        for subtypes in base_type.__args__:
            subtypes = subtypes.__args__ if isinstance(subtypes, UnionType) else [subtypes] # noqa

            if any([t in (dict, list, set, tuple, frozenset) for t in subtypes]):
                raise RecursionError("Cannot have recursive sequences and dicts")

            are_primitives = [t in PRIMITIVES for t in subtypes]
            if all(are_primitives):
                continue

            if any(are_primitives):
                raise TypeError("Can't mix between primitive and complex types in sequences/dicts")

            for type_ in subtypes:
                if type_ is NoneType:
                    continue
                tables += cls.disassemble_type(type_, depth+1)
        return tables


    @classmethod
    def _disassemble_union_type(cls, type_: UnionType, depth: int) -> list[Table]:
        tables = []
        if any([t in PRIMITIVES for t in type_.__args__]):
            raise TypeError(
                f"Cannot save object with mixed primitive and custom type annotations \
or multiple primitives! Got: {type_}"
            )
        for t in type_.__args__:
            if t is NoneType:
                continue

            if isinstance(t, GenericAlias):
                tables += cls._disassemble_generic_alias(t, depth)
            else:
                tables += cls.disassemble_type(t, depth+1)
        return tables


    @classmethod
    def disassemble_type(cls, obj_type: type, depth: int = 0) -> list[Table]:
        if depth > cls.max_depth:
            raise RecursionError(f"Surpassed max depth when disassembling type {obj_type}")

        if obj_type is Any or obj_type is NoneType:
            raise TypeError("'Any' or 'None' types are not supported!")

        if not isinstance(obj_type, type):
            raise TypeError("Passed argument must be a type!")

        if obj_type in PRIMITIVES:
            raise TypeError("Object type to disassemble cannot be a primitive type")
        tables = [Table(obj_type)]

        members: dict[str, type | UnionType | GenericAlias] = {
            key: type_
            for key, type_
            in obj_type.__annotations__.items()
            if key[0] != "__"
        }

        for key, type_ in members.items():
            tables[0].add_member(key, type_)
            if type_ not in PRIMITIVES:
                try:
                    if isinstance(type_, UnionType):
                        tables += cls._disassemble_union_type(type_, depth)
                    elif isinstance(type_, GenericAlias):
                        tables += cls._disassemble_generic_alias(type_, depth)
                    else:
                        tables += cls.disassemble_type(type_, depth+1)
                except RecursionError as err:
                    raise RecursionError(
                        f"Surpassed max depth when disassembling type {obj_type}"
                    ) from err

        return tables
