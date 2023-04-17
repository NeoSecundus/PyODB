import sqlite3 as sql
from pathlib import Path
from test.test_models.complex_models import ComplexBasic, ComplexContainer, ComplexMulti
from test.test_models.primitive_models import PrimitiveBasic, PrimitiveContainer
from unittest import TestCase

from pyodb.error import DisassemblyError, ParentError, UnknownTypeError
from pyodb.schema._base_schema import BaseSchema
from pyodb.schema.base._operators import Disassembler
from pyodb.schema.base._sql_builders import Delete, Select
from pyodb.schema.unified_schema import UnifiedSchema


class BaseSchemaTest(TestCase):
    def setUp(self) -> None:
        self.schema = UnifiedSchema(Path(".pyodb"), 2, False)
        return super().setUp()


    def test_schema_size(self):
        self.schema.add_type(ComplexBasic)
        self.assertEqual(self.schema.schema_size, 3)


    def test_schema_max_depth(self):
        self.assertEqual(self.schema.max_depth, 2)
        self.assertRaises(ValueError, setattr, self.schema, "max_depth", -1)


    def test_is_known_type(self):
        self.schema.add_type(ComplexBasic)

        self.assertTrue(self.schema.is_known_type(ComplexBasic))
        self.assertTrue(self.schema.is_known_type(PrimitiveBasic))
        self.assertFalse(self.schema.is_known_type(ComplexMulti))


    def test_not_implemented(self):
        schema = BaseSchema(Path(".pyodb"), 2, False)
        self.assertRaises(NotImplementedError, schema.add_type, PrimitiveBasic)
        self.assertRaises(NotImplementedError, schema._save_schema)


    def test_has_parent(self):
        self.schema.add_type(ComplexBasic)
        self.assertIsNotNone(self.schema.get_parent(PrimitiveBasic))
        self.assertIsNone(self.schema.get_parent(ComplexBasic))

        self.schema.add_type(ComplexContainer)
        self.assertIsNone(self.schema.get_parent(ComplexContainer))

        self.schema.add_type(ComplexMulti)
        self.assertIsNotNone(self.schema.get_parent(PrimitiveBasic))
        self.assertIsNone(self.schema.get_parent(ComplexMulti))

        self.schema.remove_type(ComplexBasic)
        self.assertRaises(UnknownTypeError, self.schema.get_parent, ComplexBasic)


    def test_remove_type(self):
        Path(".pyodb/pyodb.db").unlink(True)

        self.schema.add_type(ComplexBasic)
        self.schema.add_type(ComplexMulti)
        self.schema.add_type(ComplexContainer)

        self.schema.remove_type(ComplexBasic)
        self.assertIn(PrimitiveBasic, self.schema._tables.keys())
        self.assertIn(ComplexContainer, self.schema._tables.keys())
        self.assertIn(ComplexMulti, self.schema._tables.keys())
        self.assertIn(PrimitiveContainer, self.schema._tables.keys())
        self.assertNotIn(ComplexBasic, self.schema._tables.keys())

        self.schema.remove_type(ComplexMulti)
        self.assertIn(ComplexContainer, self.schema._tables.keys())
        self.assertNotIn(PrimitiveBasic, self.schema._tables.keys())
        self.assertNotIn(PrimitiveContainer, self.schema._tables.keys())
        self.assertNotIn(ComplexMulti, self.schema._tables.keys())
        self.assertNotIn(ComplexBasic, self.schema._tables.keys())

        self.schema.remove_type(ComplexContainer)
        self.assertEqual(len(self.schema._tables), 0)


    def test_remove_type_errors(self):
        Path(".pyodb/pyodb.db").unlink(True)
        self.schema.add_type(ComplexBasic)
        self.assertRaises(ParentError, self.schema.remove_type, PrimitiveBasic)
        self.assertRaises(UnknownTypeError, self.schema.remove_type, ComplexMulti)


    def test_insert(self):
        dbconn = sql.connect(".pyodb/pyodb.db")

        self.schema.add_type(ComplexBasic)
        self.schema.add_type(ComplexMulti)

        self.schema.insert(ComplexMulti(), None)
        self.schema.insert(ComplexBasic(), None)
        self.schema.insert(ComplexBasic(), None)

        count: int = dbconn.execute(
            "SELECT COUNT(*) FROM \"test.test_models.complex_models.ComplexBasic\";"
        ).fetchone()[0]
        self.assertEqual(count, 2)

        count1: int = dbconn.execute(
            "SELECT COUNT(*) FROM \"test.test_models.primitive_models.PrimitiveBasic\";"
        ).fetchone()[0]
        self.assertIn(count1, (3,2))

        count2: int = dbconn.execute(
            "SELECT COUNT(*) FROM \"test.test_models.primitive_models.PrimitiveContainer\";"
        ).fetchone()[0]
        self.assertIn(count2, (3,2))

        self.assertEqual(count1 + count2, 5)


    def test_insert_many(self):
        dbconn = sql.connect(".pyodb/pyodb.db")
        self.schema.add_type(ComplexBasic)

        cb = [ComplexBasic()]*10
        self.schema.insert_many(cb, None)

        count: int = dbconn.execute(
            "SELECT COUNT(*) FROM \"test.test_models.complex_models.ComplexBasic\";"
        ).fetchone()[0]
        self.assertEqual(count, 10)

        count: int = dbconn.execute(
            "SELECT COUNT(*) FROM \"test.test_models.primitive_models.PrimitiveBasic\";"
        ).fetchone()[0]
        self.assertEqual(count, 10)

        count: int = dbconn.execute(
            "SELECT COUNT(*) FROM \"test.test_models.primitive_models.PrimitiveContainer\";"
        ).fetchone()[0]
        self.assertEqual(count, 10)


    def test_insert_errors(self):
        Path(".pyodb/test.db").unlink(True)

        self.assertRaises(UnknownTypeError, self.schema.insert, ComplexContainer(), None)
        self.assertRaises(UnknownTypeError, self.schema.insert_many, [ComplexContainer()], None)

        self.schema.add_type(ComplexBasic)
        self.assertRaises(DisassemblyError, self.schema.insert_many, [ComplexBasic(), PrimitiveBasic()], None)


    def test_select(self):
        Path(".pyodb/pyodb.db").unlink(True)
        self.schema = UnifiedSchema(Path(".pyodb"), 2, False)
        self.schema.add_type(PrimitiveBasic)

        self.assertEqual(type(self.schema.select(PrimitiveBasic)), Select)
        self.assertRaises(UnknownTypeError, self.schema.select, ComplexBasic)


    def test_delete(self):
        Path(".pyodb/pyodb.db").unlink(True)
        self.schema = UnifiedSchema(Path(".pyodb"), 2, False)
        self.schema.add_type(PrimitiveBasic)

        self.assertEqual(type(self.schema.delete(PrimitiveBasic)), Delete)
        self.assertRaises(UnknownTypeError, self.schema.delete, ComplexBasic)


    def test_clear(self):
        Path(".pyodb/pyodb.db").unlink(True)
        self.schema = UnifiedSchema(Path(".pyodb"), 2, False)
        self.schema.add_type(ComplexBasic)

        self.schema.clear()
        self.assertTrue(True)
