from pathlib import Path
from unittest import TestCase
import os

from src.pyodb.schema.shard_schema import ShardSchema
from test.test_models.primitive_models import PrimitiveBasic, PrimitiveContainer
from test.test_models.complex_models import ComplexBasic


class ShardSchemaTest(TestCase):
    def setUp(self) -> None:
        self.base_path = Path(".pyodb")
        self.base_path.mkdir(755, exist_ok=True)
        self.schema = ShardSchema(self.base_path, 2, False)
        return super().setUp()


    def tearDown(self) -> None:
        del self.schema
        return super().tearDown()


    def test_add_type(self):
        self.schema.add_type(ComplexBasic)

        self.assertIn(ComplexBasic, self.schema._tables)
        self.assertIn(PrimitiveBasic, self.schema._tables)
        self.assertIn(PrimitiveContainer, self.schema._tables)

        for _, table in self.schema._tables.items():
            dbpath = Path(".pyodb/" + table.name + ".db")
            self.assertTrue(dbpath.is_file())


    def test_add_known_type(self):
        self.schema.add_type(ComplexBasic)
        self.schema.add_type(PrimitiveBasic)
        self.assertTrue(self.schema._tables[PrimitiveBasic].is_parent)
