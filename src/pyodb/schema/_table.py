"""The main module which processes python primitives (and lists and dicts) and generates sql
statements.
Including create and remove table statements in case the fields contained by a class were changed.
"""
class Table:
    # SQLITE3 Available Data Types:
    # TEXT = str, list, dict
    # NUMERIC = int, float, date, datetime
    #   ^Converts float to int if float does not have decimal places (10.01 => float, 10.0 = int)
    # INTEGER = int
    # REAL = float
    # BLOB = binary or undefined type

    def __init__(self, base_type: type) -> None:
        self._members: dict[str, type] = {}
        self.base_type = base_type


    def add_member(self, name: str, type_: type):
        """Adds a new member to the internal members

        Args:
            name (str): Name of the member (field name)
            type_ (type): Member's datatype
        """
        self._members[name] = type_


    def insert(self, obj: object):
        pass


    def delete(self, obj: object):
        pass


    def select(self, obj: object):
        pass


    def update(self, obj: object):
        pass


    def __repr__(self) -> str:
        return f"{self.base_type.__name__}: { {k: t.__name__ for k, t in self._members.items()} }"
