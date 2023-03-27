from unittest import TestCase

from src.pyodb.schema._unified_schema import UnifiedSchema
from test.test_models.primitive_models import PrimitiveBasic, PrimitiveContainer
from test.test_models.complex_models import ComplexBasic


class TableTest(TestCase):
    def setUp(self) -> None:
        self.schema = UnifiedSchema(None, 0)
        self.tpbasic = self.schema._disassemble_type(PrimitiveBasic)[0]
        self.tpcontainer = self.schema._disassemble_type(PrimitiveContainer)[0]
        self.tcbasic = self.schema._disassemble_type(ComplexBasic)[0]
        return super().setUp()


    def test_incompatible_type_errors(self):
        container = PrimitiveContainer()

        self.assertRaises(TypeError, self.tpbasic.select, container)
        self.assertRaises(TypeError, self.tpbasic.insert, container)
        self.assertRaises(TypeError, self.tpbasic.update, container)
        self.assertRaises(TypeError, self.tpbasic.delete, container)


    def test_primitive_basic_table(self):
        self.assertEqual(self.tpbasic.create_table_sql(), PrimitiveBasic.get_create_table_sql())
        self.assertEqual(self.tpbasic.drop_table_sql(), PrimitiveBasic.get_drop_table_sql())


    def test_repr(self):
        expected = "PrimitiveBasic: {'integer': 'int', 'number': 'float | None', 'text': \
'str', 'truth': 'bool', '_private': 'float', 'classmember': 'str'}"
        self.assertEqual(str(self.tpbasic), expected)
