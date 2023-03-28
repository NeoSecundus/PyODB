from random import randint
from typing import Any
from test.test_models.primitive_models import PrimitiveBasic, PrimitiveContainer, PrimitiveIllegal1


class ComplexBasic:
    random_number: int
    basic: PrimitiveBasic
    container: PrimitiveContainer

    def __init__(self) -> None:
        self.random_number = randint(-500, 500)
        self.basic = PrimitiveBasic()
        self.container = PrimitiveContainer()


    @staticmethod
    def get_members() -> dict:
        return {
            "random_number": int,
            "basic": PrimitiveBasic,
            "container": PrimitiveContainer
        }


class ComplexMulti:
    txt: str | None
    multi: PrimitiveBasic | PrimitiveContainer

    def __init__(self) -> None:
        self.txt = "Some example text"
        self.multi = PrimitiveContainer() if randint(0, 1) == 1 else PrimitiveBasic()


    @staticmethod
    def get_members() -> dict:
        return {
            "txt": str | None,
            "multi": PrimitiveBasic | PrimitiveContainer
        }


class ComplexContainer:
    complex_list: list[PrimitiveBasic]
    complex_dict: dict[str, PrimitiveBasic | None]
    multicomplex_dict: dict[str, PrimitiveBasic | PrimitiveContainer]

    def __init__(self) -> None:
        self.complex_list = []
        self.complex_dict = {}
        self.multicomplex_dict = {}

        for _ in range(randint(2, 10)):
            self.complex_list += [PrimitiveBasic()]
            self.complex_dict[self.complex_list[-1].text] = PrimitiveBasic()
            self.multicomplex_dict[self.complex_list[-1].text[::-1]] = (
                PrimitiveBasic() if randint(0, 1) == 1 else PrimitiveContainer()
            )


    @staticmethod
    def get_members() -> dict:
        return {
            "complex_list":  list[PrimitiveBasic],
            "complex_dict": dict[str, PrimitiveBasic | None],
            "multicomplex_dict": dict[str, PrimitiveBasic | PrimitiveContainer]
        }


class ComplexIllegal1:
    illegal: PrimitiveBasic | str

class ComplexIllegal2:
    illegal: PrimitiveIllegal1

class ComplexIllegal3:
    illegal: Any

class ComplexIllegal4:
    illegal: list[str | PrimitiveBasic]
