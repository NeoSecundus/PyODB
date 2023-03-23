"""Disassembles python object down to their primitive parts and creates tables accordingly.
"""
from src.pyodb.schema._table import Table
from src.pyodb.schema._table_schema import PRIMITIVES


def disassemble(obj: object, max_depth: int = 3, depth: int = 0) -> list[Table]:
    if depth > max_depth:
        return []

    obj_type = type(obj)
    if obj_type in PRIMITIVES:
        raise TypeError("Saved object cannot be a primitive type")
    tables = [Table(obj_type)]

    members: dict[str, type] = obj_type.__annotations__
    members |= {key: type(var) for key, var in vars(obj).items()}

    for key, type_ in members.items():
        if type_ in PRIMITIVES:
            tables[0].add_member(key, type_)
        else:
            tables += disassemble(getattr(obj, key), max_depth, depth+1)

    return tables
