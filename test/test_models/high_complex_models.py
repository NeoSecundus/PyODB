from random import randint
from test.test_models.complex_models import ComplexPydantic
from test.test_models.primitive_models import PrimitiveBasic, PrimitivePydantic, get_random_text


class HighComplexL1:
    random_number: int
    basic: PrimitiveBasic
    basiclist: list[PrimitiveBasic]
    reassembled = False
    pyd: ComplexPydantic


    def __odb_reassemble__(self):
        self.reassembled = True


    def __init__(self) -> None:
        self.random_number = randint(-1000000, 1000000)
        self.basiclist = [PrimitiveBasic() for _ in range(randint(3, 10))]
        self.basic = PrimitiveBasic()
        self.reassembled = False
        self.pyd = ComplexPydantic(
            child=PrimitivePydantic(
                test_bool=False,
                test_float=9.25,
                test_str="Test Child",
                test_number=50
            ),
            test_str="Test Parent"
        )


    def __eq__(self, __o: object) -> bool:
        if isinstance(__o, HighComplexL1):
            return (
                self.random_number ==  __o.random_number
                and self.basic == __o.basic
                and self.basiclist == __o.basiclist
            )
        return False


class HighComplexL2:
    mytext: str
    high1: HighComplexL1
    high2: PrimitiveBasic | None

    def __init__(self) -> None:
        self.mytext = get_random_text(20)
        self.high1 = HighComplexL1()
        self.high2 = None if randint(0, 2) == 1 else PrimitiveBasic()


    def __eq__(self, __o: object) -> bool:
        if isinstance(__o, HighComplexL2):
            return (
                self.mytext ==  __o.mytext
                and self.high1 == __o.high1
                and self.high2 == __o.high2
            )
        return False

class HighComplexL3:
    mytext: str
    mylist: list[str]
    high0: PrimitiveBasic | None
    high1: HighComplexL1
    high2: HighComplexL2

    def __init__(self) -> None:
        self.mytext = get_random_text(20)
        self.mylist = [get_random_text(20) for _ in range(5)]
        self.high0 = None if randint(0, 2) == 1 else PrimitiveBasic()
        self.high1 = HighComplexL1()
        self.high2 = HighComplexL2()


    def __eq__(self, __o: object) -> bool:
        if isinstance(__o, HighComplexL3):
            return (
                self.mytext ==  __o.mytext
                and self.high0 == __o.high0
                and self.high1 == __o.high1
                and self.high2 == __o.high2
            )
        return False
