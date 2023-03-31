from pathlib import Path
from unittest import TestCase
import os

from src.pyodb.schema._shard_schema import ShardSchema
from test.test_models.primitive_models import PrimitiveBasic, PrimitiveContainer
from test.test_models.complex_models import ComplexBasic


class ShardSchemaTest(TestCase):
    def setUp(self) -> None:
        self.base_path = Path(".pyodb")
        self.base_path.mkdir(755, exist_ok=True)

        for c in [ComplexBasic, PrimitiveBasic, PrimitiveContainer]:
            c: type
            dbpath = self.base_path / (c.__name__ + ".db")
            if dbpath.exists():
                os.remove(dbpath)
        return super().setUp()


    def test_add_type(self):
        schema = ShardSchema(self.base_path, 2)
        schema.add_type(ComplexBasic)

        self.assertIn(ComplexBasic, schema._tables)
        self.assertIn(PrimitiveBasic, schema._tables)
        self.assertIn(PrimitiveContainer, schema._tables)

        for _, table in schema._tables.items():
            dbpath = Path(".pyodb/" + table.name + ".db")
            self.assertTrue(dbpath.is_file())


    def test_add_known_type(self):
        schema = ShardSchema(self.base_path, 2)
        schema.add_type(ComplexBasic)
        schema.add_type(PrimitiveBasic)
        self.assertTrue(schema._tables[PrimitiveBasic].is_parent)
