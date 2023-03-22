"""Disassembles python object down to their primitive parts and creates tables accordingly.
"""
import inspect

from src.pyodb.schema._table import Table


def disassemble(obj: object, depth: int = 0, max_depth: int = 3) -> list[Table]:
    if depth > max_depth:
        return []

    obj_type = type(obj)
    tables = [Table(obj_type)]

    members: dict[str, type] = inspect.get_annotations(obj_type)
    members |= {key: type(var) for key, var in vars(obj).items()}

    for key, type_ in members.items():
        if type_ in [int, str, bool, float, list, set, dict]:
            tables[0].add_member(key, type_)
        else:
            tables += disassemble(getattr(obj, key), depth+1, max_depth)

    return tables
