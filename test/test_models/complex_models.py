from random import randint

from pydantic import BaseModel
from test.test_models.primitive_models import (PrimitiveBasic, PrimitiveContainer,
                                               PrimitiveIllegal1, PrimitivePydantic, get_random_text)
from typing import Any


class ComplexBasic:
    random_number: int
    basic: PrimitiveBasic
    container: PrimitiveContainer

    def __init__(self) -> None:
        self.random_number = randint(-1000000, 1000000)
        self.basic = PrimitiveBasic()
        self.container = PrimitiveContainer()


    @staticmethod
    def get_members() -> dict:
        return {
            "random_number": int,
            "basic": PrimitiveBasic,
            "container": PrimitiveContainer
        }


    def __eq__(self, __o: object) -> bool:
        if isinstance(__o, ComplexBasic):
            return (
                self.random_number ==  __o.random_number
                and self.basic == __o.basic
                and self.container == __o.container
            )
        return False


class ComplexMulti:
    txt: str
    multi: PrimitiveBasic | PrimitiveContainer

    def __init__(self) -> None:
        self.txt = get_random_text(30)
        self.multi = PrimitiveContainer() if randint(0, 1) == 1 else PrimitiveBasic()


    @staticmethod
    def get_members() -> dict:
        return {
            "txt": str,
            "multi": PrimitiveBasic | PrimitiveContainer
        }


    def __eq__(self, __o: object) -> bool:
        if isinstance(__o, ComplexMulti):
            return self.txt == __o.txt and self.multi == __o.multi
        return False


class ComplexContainer:
    complex_list: list[PrimitiveBasic] | None
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
        if randint(0, 2) == 1:
            self.complex_list = None



    @staticmethod
    def get_members() -> dict:
        return {
            "complex_list":  list | None,
            "complex_dict": dict,
            "multicomplex_dict": dict
        }


class ComplexPydantic(BaseModel):
    child: PrimitivePydantic
    test_str: str

    @staticmethod
    def get_members() -> dict:
        return {
            "child": PrimitivePydantic,
            "test_str": str
        }


class ComplexIllegal1:
    illegal: PrimitiveBasic | str

class ComplexIllegal2:
    illegal: PrimitiveIllegal1

class ComplexIllegal3:
    illegal: Any
