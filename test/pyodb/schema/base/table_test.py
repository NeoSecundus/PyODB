from pathlib import Path
from test.test_models.complex_models import ComplexBasic
from test.test_models.primitive_models import PrimitiveBasic, PrimitiveContainer
from unittest import TestCase

from pyodb.schema.unified_schema import UnifiedSchema


class TableTest(TestCase):
    def setUp(self) -> None:
        _schema = UnifiedSchema(Path(".pyodb"), 0, False)
        _schema.add_type(PrimitiveBasic)
        self.tpbasic = _schema._tables[PrimitiveBasic]

        _schema.add_type(PrimitiveContainer)
        self.tpcontainer = _schema._tables[PrimitiveContainer]

        _schema.add_type(ComplexBasic)
        self.tcbasic = _schema._tables[ComplexBasic]
        return super().setUp()


    def test_primitive_basic_table(self):
        self.assertEqual(self.tpbasic._create_table_sql(), PrimitiveBasic.get_create_table_sql())
        self.assertEqual(self.tpbasic._drop_table_sql(), PrimitiveBasic.get_drop_table_sql())


    def test_primitive_container_table(self):
        self.assertEqual(self.tpcontainer._create_table_sql(), PrimitiveContainer.get_create_table_sql())
        self.assertEqual(self.tpcontainer._drop_table_sql(), PrimitiveContainer.get_drop_table_sql())


    def test_repr(self):
        expected = "PrimitiveBasic: {'integer': 'int', 'number': 'float | None', 'text': \
'str', 'truth': 'bool', '_private': 'float'};"
        self.assertEqual(str(self.tpbasic), expected)


    def test_fqcn(self):
        self.assertEqual(self.tpbasic.fqcn, "test.test_models.primitive_models.PrimitiveBasic")
        self.assertEqual(self.tpbasic.name, "PrimitiveBasic")
