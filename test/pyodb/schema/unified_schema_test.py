from pathlib import Path
from unittest import TestCase

from src.pyodb.schema._unified_schema import UnifiedSchema
from test.test_models.primitive_models import PrimitiveBasic, PrimitiveContainer
from test.test_models.complex_models import ComplexBasic


class UnifiedSchemaTest(TestCase):
    def test_add_type(self):
        Path(".pyodb/pyodb.db").unlink(True)
        schema = UnifiedSchema(Path(".pyodb"), 2)
        schema.add_type(ComplexBasic)

        self.assertIn(ComplexBasic, schema._tables)
        self.assertIn(PrimitiveBasic, schema._tables)
        self.assertIn(PrimitiveContainer, schema._tables)

        for _, table in schema._tables.items():
            self.assertEqual(schema._dbconn, table.dbconn)
