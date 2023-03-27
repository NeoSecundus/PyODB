from random import random, randint, choice

def _get_random_text(limit: int = 100) -> str:
    allowed_chars = " abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789=!\"§$%&/()\\`´\
#'+*~-_.:,;<>|[]}{^°"
    return "".join(choice(allowed_chars) for _ in range(randint(1, limit)))


class PrimitiveBasic:
    integer: int
    number: float | None
    text: str
    truth: bool
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
        self.number = None if randint(0, 1) == 1 else (random()-0.5) * 2000
        self.text = _get_random_text()
        self.truth = randint(0, 1) == 1
        self._private = random()


    def test_func(self) -> str:
        return self.text


    @staticmethod
    def get_create_table_sql() -> str:
        return "CREATE TABLE pyodb_test_test_models_primitive_models_PrimitiveBasic (\
_uid_ TEXT PRIMARY KEY,_members_ TEXT,_parent_ TEXT,_pickle_ BLOB NOT NULL,\
integer INTEGER NOT NULL,number REAL,text TEXT NOT NULL,truth INTEGER NOT NULL,\
_private REAL NOT NULL,classmember TEXT NOT NULL);"


    @staticmethod
    def get_drop_table_sql() -> str:
        return "DROP TABLE pyodb_test_test_models_primitive_models_PrimitiveBasic;"


    @staticmethod
    def get_members() -> dict:
        return {
            "integer": int,
            "number": float | None,
            "text": str,
            "truth": bool,
            "_private": float,
            "classmember": str
        }


    def __repr__(self) -> str:
        return f"PrimitiveBasic: {self.integer}, {self.number}, '{self.text}', {self.truth}"


class PrimitiveContainer:
    listing: list[int | float | str | bool]
    pset: set[int | float | str | bool]
    ptuple: tuple[int | float | str | bool]
    dictionary: dict[str, int | float | str | bool]


    def __init__(self) -> None:
        self.listing = []
        self.listing += [randint(-1000,1000) for _ in range(randint(1, 10))]
        self.listing += [(random()-0.5) * 2000 for _ in range(randint(1, 10))]
        self.listing += [_get_random_text() for _ in range(randint(1, 10))]
        self.listing += [randint(0, 1) == 1 for _ in range(randint(1, 10))]

        self.pset = set()
        self.pset.union([randint(-1000,1000) for _ in range(randint(1, 10))])
        self.pset.union([(random()-0.5) * 2000 for _ in range(randint(1, 10))])
        self.pset.union([_get_random_text() for _ in range(randint(1, 10))])
        self.pset.union([randint(0, 1) == 1 for _ in range(randint(1, 10))])

        self.ptuple = tuple([randint(-10, 10), (random()-0.5) * 20, _get_random_text(), randint(0, 1) == 1])

        self.dictionary = {}
        self.dictionary |= {_get_random_text(10): randint(-1000,1000) for _ in range(randint(1, 10))}
        self.dictionary |= {_get_random_text(10): (random()-0.5) * 2000 for _ in range(randint(1, 10))}
        self.dictionary |= {_get_random_text(10): _get_random_text() for _ in range(randint(1, 10))}
        self.dictionary |= {_get_random_text(10): randint(0, 1) == 1 for _ in range(randint(1, 10))}


    def __repr__(self) -> str:
        return f"PrimitiveContainer:\n{self.listing}\n{self.dictionary}"


    @staticmethod
    def get_members() -> dict:
        return {
            "listing": list,
            "pset": set,
            "ptuple": tuple,
            "dictionary": dict,
        }


class PrimitiveIllegal:
    illegal: int | float
