from pathlib import Path
from unittest import TestCase
import sqlite3 as sql

from src.pyodb.schema._base_schema import BaseSchema
from src.pyodb.schema.base._operators import Disassembler
from test.test_models.primitive_models import PrimitiveBasic, PrimitiveContainer
from test.test_models.complex_models import ComplexBasic, ComplexContainer, ComplexMulti


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


    def test_has_parent(self):
        tables = Disassembler.disassemble_type(ComplexBasic)
        tables[0].is_parent = True
        self.schema._tables = {t.base_type: t for t in tables}
        self.assertIsNotNone(self.schema.get_parent(PrimitiveBasic))
        self.assertIsNone(self.schema.get_parent(ComplexBasic))

        tables = Disassembler.disassemble_type(ComplexContainer)
        tables[0].is_parent = True
        self.schema._tables = {t.base_type: t for t in tables}
        self.assertIsNone(self.schema.get_parent(ComplexContainer))

        tables = Disassembler.disassemble_type(ComplexMulti)
        tables[0].is_parent = True
        self.schema._tables = {t.base_type: t for t in tables}
        self.assertIsNotNone(self.schema.get_parent(PrimitiveBasic))
        self.assertIsNone(self.schema.get_parent(ComplexMulti))

        self.assertRaises(TypeError, self.schema.get_parent, ComplexBasic)


    def test_remove_type(self):
        Path(".pyodb/test.db").unlink(True)
        tables = Disassembler.disassemble_type(ComplexBasic)
        tables[0].is_parent = True
        tables = [Disassembler.disassemble_type(ComplexContainer)[0]] + tables
        tables[0].is_parent = True
        tables = [Disassembler.disassemble_type(ComplexMulti)[0]] + tables
        tables[0].is_parent = True
        dbconn = sql.connect(".pyodb/test.db")

        for table in tables:
            self.schema._tables[table.base_type] = table
            table.dbconn = dbconn
            table.create_table()

        self.schema.remove_type(ComplexBasic)
        self.assertIn(PrimitiveBasic, self.schema._tables.keys())
        self.assertIn(ComplexContainer, self.schema._tables.keys())
        self.assertIn(ComplexMulti, self.schema._tables.keys())
        self.assertIn(PrimitiveContainer, self.schema._tables.keys())
        self.assertNotIn(ComplexBasic, self.schema._tables.keys())

        self.schema.remove_type(ComplexMulti)
        self.assertIn(PrimitiveBasic, self.schema._tables.keys())
        self.assertIn(ComplexContainer, self.schema._tables.keys())
        self.assertIn(PrimitiveContainer, self.schema._tables.keys())
        self.assertNotIn(ComplexMulti, self.schema._tables.keys())
        self.assertNotIn(ComplexBasic, self.schema._tables.keys())

        self.schema.remove_type(ComplexContainer)
        self.assertEqual(len(self.schema._tables), 0)


    def test_remove_type_errors(self):
        Path(".pyodb/test.db").unlink(True)
        tables = Disassembler.disassemble_type(ComplexBasic)
        tables[0].is_parent = True
        dbconn = sql.connect(".pyodb/test.db")

        for table in tables:
            self.schema._tables[table.base_type] = table
            table.dbconn = dbconn
            table.create_table()

        self.assertRaises(ConnectionError, self.schema.remove_type, PrimitiveBasic)
        self.assertRaises(TypeError, self.schema.remove_type, ComplexMulti)


    def test_insert(self):
        Path(".pyodb/test.db").unlink(True)
        tables = Disassembler.disassemble_type(ComplexBasic)
        tables[0].is_parent = True
        tables += [Disassembler.disassemble_type(ComplexMulti)[0]]
        tables[-1].is_parent = True
        dbconn = sql.connect(".pyodb/test.db")

        for table in tables:
            self.schema._tables[table.base_type] = table
            table.dbconn = dbconn
            table.create_table()

        self.schema.insert(ComplexMulti())
        self.schema.insert(ComplexBasic())
        self.schema.insert(ComplexBasic())

        count: int = dbconn.execute(
            "SELECT COUNT(*) FROM test_test_models_complex_models_ComplexBasic;"
        ).fetchone()[0]
        self.assertEqual(count, 2)

        count1: int = dbconn.execute(
            "SELECT COUNT(*) FROM test_test_models_primitive_models_PrimitiveBasic;"
        ).fetchone()[0]
        self.assertIn(count1, (3,2))

        count2: int = dbconn.execute(
            "SELECT COUNT(*) FROM test_test_models_primitive_models_PrimitiveContainer;"
        ).fetchone()[0]
        self.assertIn(count2, (3,2))

        self.assertEqual(count1 + count2, 5)


    def test_insert_many(self):
        Path(".pyodb/test.db").unlink(True)
        tables = Disassembler.disassemble_type(ComplexBasic)
        tables[0].is_parent = True
        dbconn = sql.connect(".pyodb/test.db")

        for table in tables:
            self.schema._tables[table.base_type] = table
            table.dbconn = dbconn
            table.create_table()

        cb = [ComplexBasic()]*10
        self.schema.insert_many(cb)

        count: int = dbconn.execute(
            "SELECT COUNT(*) FROM test_test_models_complex_models_ComplexBasic;"
        ).fetchone()[0]
        self.assertEqual(count, 10)

        count: int = dbconn.execute(
            "SELECT COUNT(*) FROM test_test_models_primitive_models_PrimitiveBasic;"
        ).fetchone()[0]
        self.assertEqual(count, 10)

        count: int = dbconn.execute(
            "SELECT COUNT(*) FROM test_test_models_primitive_models_PrimitiveContainer;"
        ).fetchone()[0]
        self.assertEqual(count, 10)


    def test_insert_errors(self):
        Path(".pyodb/test.db").unlink(True)
        tables = Disassembler.disassemble_type(ComplexBasic)
        tables[0].is_parent = True
        dbconn = sql.connect(".pyodb/test.db")

        for table in tables:
            self.schema._tables[table.base_type] = table
            table.dbconn = dbconn
            table.create_table()

        self.schema._tables[ComplexBasic].dbconn = None
        self.assertRaises(ConnectionError, self.schema.insert, ComplexBasic())
        self.assertRaises(ConnectionError, self.schema.insert_many, [ComplexBasic()])

        self.schema._tables[ComplexBasic].dbconn = dbconn
        self.assertRaises(TypeError, self.schema.insert, ComplexContainer())
        self.assertRaises(TypeError, self.schema.insert_many, [ComplexContainer()])
        self.assertRaises(TypeError, self.schema.insert_many, [ComplexBasic(), PrimitiveBasic()])