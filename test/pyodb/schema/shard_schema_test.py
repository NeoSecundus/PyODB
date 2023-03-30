from pathlib import Path
from unittest import TestCase
from shutil import rmtree
import os

from src.pyodb.schema._shard_schema import ShardSchema
from test.test_models.primitive_models import PrimitiveBasic, PrimitiveContainer
from test.test_models.complex_models import ComplexBasic


class ShardSchemaTest(TestCase):
    def test_add_type(self):
        base_path = Path(".pyodb")
        rmtree(base_path.as_posix() + "/")
        base_path.mkdir(755)

        schema = ShardSchema(base_path, 2)
        schema.add_type(ComplexBasic)

        self.assertIn(ComplexBasic, schema._tables)
        self.assertIn(PrimitiveBasic, schema._tables)
        self.assertIn(PrimitiveContainer, schema._tables)

        db_list = os.listdir(base_path.as_posix())
        for _, table in schema._tables.items():
            self.assertIn(table.name + ".db", db_list)
