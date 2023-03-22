from random import randint
from test.test_models.primitive_models import PrimitiveBasic, PrimitiveContainer


class ComplexBasic:
    def __init__(self) -> None:
        self.random_number = randint(-500, 500)
        self.basic = PrimitiveBasic()
        self.container = PrimitiveContainer()
