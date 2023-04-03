from pathlib import Path
from unittest import TestCase

from src.pyodb.schema.unified_schema import UnifiedSchema
from test.test_models.primitive_models import PrimitiveBasic, PrimitiveContainer
from test.test_models.complex_models import ComplexBasic


class UnifiedSchemaTest(TestCase):
    def test_add_type(self):
        base_path = Path(".pyodb")
        (base_path / "pyodb.db").unlink(True)
        schema = UnifiedSchema(base_path, 2, False)
        schema.add_type(ComplexBasic)

        self.assertIn(ComplexBasic, schema._tables)
        self.assertIn(PrimitiveBasic, schema._tables)
        self.assertIn(PrimitiveContainer, schema._tables)

        for _, table in schema._tables.items():
            self.assertEqual(schema._tables[ComplexBasic].dbconn, table.dbconn)
        del schema
