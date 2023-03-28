from pathlib import Path
from unittest import TestCase

from src.pyodb.schema._base_schema import BaseSchema
from src.pyodb.schema.base._operators import Disassembler
from test.test_models.primitive_models import PrimitiveBasic
from test.test_models.complex_models import ComplexBasic, ComplexMulti


class BaseSchemaTest(TestCase):
    def setUp(self) -> None:
        self.schema = BaseSchema(Path(".pyodb"), 2)
        return super().setUp()


    def test_schema_size(self):
        tables = Disassembler.disassemble_type(ComplexBasic)
        self.schema._tables = {t.base_type: t for t in tables}

        self.assertEqual(self.schema.schema_size, 3)


    def test_schema_max_depth(self):
        self.assertEqual(self.schema.max_depth, 2)


    def test_is_known_type(self):
        tables = Disassembler.disassemble_type(ComplexBasic)
        self.schema._tables = {t.base_type: t for t in tables}

        self.assertTrue(self.schema.is_known_type(ComplexBasic))
        self.assertTrue(self.schema.is_known_type(PrimitiveBasic))
        self.assertFalse(self.schema.is_known_type(ComplexMulti))


    def test_not_implemented(self):
        self.assertRaises(NotImplementedError, self.schema.add_type, PrimitiveBasic)
        self.assertRaises(NotImplementedError, self.schema.remove_type, PrimitiveBasic)
