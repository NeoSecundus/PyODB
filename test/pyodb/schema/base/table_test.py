from unittest import TestCase

from src.pyodb.schema.base._operators import Disassembler
from test.test_models.primitive_models import PrimitiveBasic, PrimitiveContainer
from test.test_models.complex_models import ComplexBasic


class TableTest(TestCase):
    def setUp(self) -> None:
        Disassembler.max_depth = 2
        self.tpbasic = Disassembler.disassemble_type(PrimitiveBasic)[0]
        self.tpcontainer = Disassembler.disassemble_type(PrimitiveContainer)[0]
        self.tcbasic = Disassembler.disassemble_type(ComplexBasic)[0]
        return super().setUp()


    def test_primitive_basic_table(self):
        self.assertEqual(self.tpbasic.create_table_sql(), PrimitiveBasic.get_create_table_sql())
        self.assertEqual(self.tpbasic.drop_table_sql(), PrimitiveBasic.get_drop_table_sql())


    def test_primitive_container_table(self):
        self.assertEqual(self.tpcontainer.create_table_sql(), PrimitiveContainer.get_create_table_sql())
        self.assertEqual(self.tpcontainer.drop_table_sql(), PrimitiveContainer.get_drop_table_sql())


    def test_repr(self):
        expected = "PrimitiveBasic: {'integer': 'int', 'number': 'float | None', 'text': \
'str', 'truth': 'bool', '_private': 'float', 'classmember': 'str'}"
        self.assertEqual(str(self.tpbasic), expected)
