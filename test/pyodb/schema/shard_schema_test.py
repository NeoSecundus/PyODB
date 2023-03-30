from pathlib import Path
from unittest import TestCase
import os

from src.pyodb.schema._shard_schema import ShardSchema
from test.test_models.primitive_models import PrimitiveBasic, PrimitiveContainer
from test.test_models.complex_models import ComplexBasic


class ShardSchemaTest(TestCase):
    def setUp(self) -> None:
        self.base_path = Path(".pyodb")
        for file in os.listdir(self.base_path.as_posix()):
            os.remove(self.base_path / file)
        return super().setUp()


    def test_add_type(self):
        schema = ShardSchema(self.base_path, 2)
        schema.add_type(ComplexBasic)

        self.assertIn(ComplexBasic, schema._tables)
        self.assertIn(PrimitiveBasic, schema._tables)
        self.assertIn(PrimitiveContainer, schema._tables)

        db_list = os.listdir(self.base_path.as_posix())
        for _, table in schema._tables.items():
            self.assertIn(table.name + ".db", db_list)
