from pydantic import BaseModel
from random import choice, randint, random


def get_random_text(limit: int = 100) -> str:
    allowed_chars = " abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789=!\"§$&/()\\`´\
#'+*~-_.:,;<>|[]}{^°"
    return "".join(choice(allowed_chars) for _ in range(randint(1, limit)))


class PrimitiveBasic:
    __odb_members__: dict[str, type] = {
        "integer": int,
        "number": float | None,
        "text": str,
        "truth": bool,
        "_private": float
    }

    _private: float
    classmember: str = "cm"


    @property
    def prop_number(self) -> float | None:
        if self.number:
            return self.number * self._private
        else:
            return None


    def __init__(self) -> None:
        self.integer = randint(-1000, 1000)
        self.number = None if randint(0, 2) == 1 else (random()-0.5) * 2000
        self.text = get_random_text()
        self.truth = randint(0, 1) == 1
        self._private = random()


    def test_func(self) -> str:
        return self.text


    @staticmethod
    def get_create_table_sql() -> str:
        return "CREATE TABLE IF NOT EXISTS \"test.test_models.primitive_models.PrimitiveBasic\" (\
_uid_ TEXT PRIMARY KEY,_parent_ TEXT,_parent_table_ TEXT,_expires_ REAL,\
integer INTEGER NOT NULL,number REAL,text TEXT NOT NULL,truth INTEGER NOT NULL,\
_private REAL NOT NULL);"


    @staticmethod
    def get_drop_table_sql() -> str:
        return "DROP TABLE IF EXISTS \"test.test_models.primitive_models.PrimitiveBasic\";"


    @staticmethod
    def get_members() -> dict:
        return {
            "integer": int,
            "number": float | None,
            "text": str,
            "truth": bool,
            "_private": float
        }


    def __repr__(self) -> str:
        return f"PrimitiveBasic: {self.integer}, {self.number}, '{self.text}', {self.truth}"


    def __eq__(self, __o: object) -> bool:
        if isinstance(__o, PrimitiveBasic):
            return all([val == getattr(__o, key) for key, val in vars(self).items()])
        return False


class PrimitiveContainer:
    listing: list[int | float | str | bool]
    pset: set[int | float | str | bool]
    ptuple: tuple[int | float | str | bool] | None
    dictionary: dict[str, int | float | str | bool]


    def __init__(self) -> None:
        self.listing = []
        self.listing += [randint(-1000,1000) for _ in range(randint(1, 10))]
        self.listing += [(random()-0.5) * 2000 for _ in range(randint(1, 10))]
        self.listing += [get_random_text() for _ in range(randint(1, 10))]
        self.listing += [randint(0, 1) == 1 for _ in range(randint(1, 10))]

        self.pset = set()
        self.pset.union([randint(-1000,1000) for _ in range(randint(1, 10))])
        self.pset.union([(random()-0.5) * 2000 for _ in range(randint(1, 10))])
        self.pset.union([get_random_text() for _ in range(randint(1, 10))])
        self.pset.union([randint(0, 1) == 1 for _ in range(randint(1, 10))])

        self.ptuple = tuple([randint(-10, 10), (random()-0.5) * 20, get_random_text(), randint(0, 1) == 1])

        self.dictionary = {}
        self.dictionary |= {get_random_text(10): randint(-1000,1000) for _ in range(randint(1, 10))}
        self.dictionary |= {get_random_text(10): (random()-0.5) * 2000 for _ in range(randint(1, 10))}
        self.dictionary |= {get_random_text(10): get_random_text() for _ in range(randint(1, 10))}
        self.dictionary |= {get_random_text(10): randint(0, 1) == 1 for _ in range(randint(1, 10))}


    def __repr__(self) -> str:
        return f"PrimitiveContainer:\n{self.listing}\n{self.dictionary}"


    @staticmethod
    def get_members() -> dict:
        return {
            "listing": list,
            "pset": set,
            "ptuple": tuple | None,
            "dictionary": dict,
        }

    @staticmethod
    def get_create_table_sql() -> str:
        return "CREATE TABLE IF NOT EXISTS \"test.test_models.primitive_models.PrimitiveContainer\" (\
_uid_ TEXT PRIMARY KEY,_parent_ TEXT,_parent_table_ TEXT,_expires_ REAL,\
listing BLOB NOT NULL,pset BLOB NOT NULL,ptuple BLOB,dictionary BLOB NOT NULL);"


    @staticmethod
    def get_drop_table_sql() -> str:
        return "DROP TABLE IF EXISTS \"test.test_models.primitive_models.PrimitiveContainer\";"


    def __eq__(self, __o: object) -> bool:
        if isinstance(__o, PrimitiveContainer):
            return self.listing == __o.listing
        return False


class PrimitivePydantic(BaseModel):
    test_str: str
    test_float: float
    test_number: int
    test_bool: bool

    @staticmethod
    def get_members() -> dict:
        return {
            "test_str": str,
            "test_float": float,
            "test_number": int,
            "test_bool": bool
        }


class ReassemblyTester:
    txt: str
    reassembled: bool

    def __init__(self) -> None:
        self.txt = get_random_text(30)
        self.reassembled = False


    def __odb_reassemble__(self):
        self.reassembled = True


class PrimitiveIllegal1:
    illegal: int | float


class PrimitiveIllegal2:
    illegal: list[float | str] | float | None
