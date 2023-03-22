from random import random, randint, choice

def _get_random_text(limit: int = 100) -> str:
    allowed_chars = " abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789=!\"§$%&/()\\`´\
#'+*~-_.:,;<>|[]}{^°"
    return "".join(choice(allowed_chars) for _ in range(randint(1, limit)))


class PrimitiveBasic:
    integer: int
    number: float
    text: str
    truth: bool
    _private: float
    classmember: str = "cm"


    def __init__(self) -> None:
        self.integer = randint(-1000, 1000)
        self.number = (random()-0.5) * 2000
        self.text = _get_random_text()
        self.truth = randint(0, 1) == 1
        self._private = random()


    def test_func(self) -> str:
        return self.text


    @property
    def private(self):
        return self._private


    def __repr__(self) -> str:
        return f"PrimitiveBasic: {self.integer}, {self.number}, '{self.text}', {self.truth}"


class PrimitiveContainer:
    listing: list[int | float | str | bool]
    dictionary: dict[str, int | float | str | bool]


    def __init__(self) -> None:
        self.listing = []
        self.listing += [randint(-1000,1000) for _ in range(randint(1, 10))]
        self.listing += [(random()-0.5) * 2000 for _ in range(randint(1, 10))]
        self.listing += [_get_random_text() for _ in range(randint(1, 10))]
        self.listing += [randint(0, 1) == 1 for _ in range(randint(1, 10))]

        self.dictionary = {}
        self.dictionary |= {_get_random_text(10): randint(-1000,1000) for _ in range(randint(1, 10))}
        self.dictionary |= {_get_random_text(10): (random()-0.5) * 2000 for _ in range(randint(1, 10))}
        self.dictionary |= {_get_random_text(10): _get_random_text() for _ in range(randint(1, 10))}
        self.dictionary |= {_get_random_text(10): randint(0, 1) == 1 for _ in range(randint(1, 10))}


    def __repr__(self) -> str:
        return f"PrimitiveContainer:\n{self.listing}\n{self.dictionary}"
