from logging import Logger
from unittest import TestCase
from src.pyodb.pyodb import PyODB

from test.test_models.primitive_models import PrimitiveBasic, PrimitiveContainer
from test.test_models.complex_models import ComplexBasic, ComplexMulti


class PyODBTest(TestCase):
    def setUp(self) -> None:
        self.pyodb = PyODB(sharding=True)
        return super().setUp()


    def tearDown(self) -> None:
        del self.pyodb
        return super().tearDown()


    def test_modify_logging(self):
        self.pyodb.modify_logging(False)
        self.assertIsNone(self.pyodb._logger)

        self.pyodb.modify_logging(True)
        self.assertEqual(type(self.pyodb._logger), Logger)


    def test_max_depth(self):
        self.pyodb.max_depth = 3
        self.assertEqual(self.pyodb.max_depth, 3)


    def test_set_persistent(self):
        self.pyodb.persistent = True
        self.assertTrue(self.pyodb.persistent)
        self.pyodb.persistent = False


    def test_add_remove_type(self):
        self.pyodb.add_type(PrimitiveBasic)
        self.assertIn(PrimitiveBasic, self.pyodb._schema._tables)
        self.pyodb.remove_type(PrimitiveBasic)
        self.assertNotIn(PrimitiveBasic, self.pyodb._schema._tables)


    def test_object_functions(self):
        self.pyodb.add_type(ComplexBasic)
        cb = ComplexBasic()
        self.pyodb.save(cb)
        self.pyodb.save_multiple([ComplexBasic(), ComplexBasic()])

        query = self.pyodb.select(ComplexBasic)
        res = query.first()
        self.assertEqual(cb, res)

        query = self.pyodb.delete(ComplexBasic)
        query.eq(random_number = cb.random_number).commit()

        query = self.pyodb.select(ComplexBasic)
        res = query.all()
        self.assertNotIn(cb, res)

        self.pyodb.clear()
        res = self.pyodb.select(ComplexBasic).all()
        self.assertEqual(res, [])

        self.pyodb.save_multiple([])
        self.pyodb.save(ComplexMulti())
        self.assertTrue(self.pyodb.contains_type(ComplexMulti))


    def test_contains_type(self):
        self.pyodb.add_type(ComplexMulti)
        self.assertTrue(self.pyodb.contains_type(ComplexMulti))
        self.assertRaises(TypeError, self.pyodb.contains_type, ComplexMulti())


    def test_persistency(self):
        # Test Shard Schema
        del self.pyodb
        self.pyodb = PyODB(sharding=True)
        self.pyodb.add_type(ComplexBasic)
        self.pyodb.persistent = True
        del self.pyodb

        self.pyodb = PyODB(sharding=True)
        self.assertIn(ComplexBasic, self.pyodb._schema._tables)
        self.assertIn(PrimitiveBasic, self.pyodb._schema._tables)
        self.assertIn(PrimitiveContainer, self.pyodb._schema._tables)
        del self.pyodb

        # Test Unified Schema
        self.pyodb = PyODB()
        self.pyodb.persistent = True
        self.pyodb.add_type(ComplexBasic)
        del self.pyodb

        self.pyodb = PyODB()
        self.assertIn(ComplexBasic, self.pyodb._schema._tables)
        self.assertIn(PrimitiveBasic, self.pyodb._schema._tables)
        self.assertIn(PrimitiveContainer, self.pyodb._schema._tables)
        self.pyodb.persistent = False
        del self.pyodb

        self.pyodb = PyODB()
        self.assertEqual({}, self.pyodb._schema._tables)
