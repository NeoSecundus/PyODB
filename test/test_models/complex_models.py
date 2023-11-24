from decimal import Decimal
from random import randint
import random

from pydantic import BaseModel, Field
from test.test_models.primitive_models import (PrimitiveBasic, PrimitiveContainer,
                                               PrimitiveIllegal1, PrimitivePydantic, get_random_text)
from typing import Any, Callable, ClassVar, Coroutine


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
    str_field: str = Field(default="Test")
    flt_field: float = Field(default=random.random())
    int_field: int = Field(default=random.randint(-1000, 1000))
    bol_field: bool = Field(default=False)
    sub_field: PrimitivePydantic = Field(default=PrimitivePydantic(
        test_str="Tester",
        test_float=1.1,
        test_number=100,
        test_bool=True
    ))

    @staticmethod
    def get_members() -> dict:
        return {
            "str_field": str,
            "flt_field": float,
            "int_field": int,
            "bol_field": bool,
            "sub_field": PrimitivePydantic,
        }


async def _my_async() -> str:
    return "Kill me pls"


class ComplexTypingModel:
    __odb_members__ = {
        "obj_decimal": float,
        "obj_int": int,
    }

    cls_int: ClassVar[str] = "This is a class variable"
    cls_decimal: ClassVar[Decimal] = Decimal(random.random() * 100)
    obj_decimal: Decimal
    obj_int: int
    obj_func: Callable = lambda: "help me"
    obj_coroutine: Coroutine = _my_async()

    def __init__(self) -> None:
        self.obj_decimal = Decimal(random.random() * 100)
        self.obj_int = random.randint(-1000, 1000)


    def __odb_reassemble__(self):
        self.obj_decimal = Decimal(self.obj_decimal)


class ComplexIllegal1:
    illegal: PrimitiveBasic | str

class ComplexIllegal2:
    illegal: PrimitiveIllegal1

class ComplexIllegal3:
    illegal: Any
