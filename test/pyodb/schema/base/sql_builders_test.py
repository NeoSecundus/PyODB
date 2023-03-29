from unittest import TestCase

from src.pyodb.schema.base._sql_builders import MultiInsert, Insert
from test.test_models.primitive_models import PrimitiveBasic


class MultiInsertTest(TestCase):
    def test_add(self):
        inserts = [Insert("table1", None, None)]*4
        mi = MultiInsert("table1")
        mi += inserts[0]
        mi += inserts[1]
        mi2 = MultiInsert("table1")
        mi2 += inserts[2]
        mi2 += inserts[3]
        mi += mi2

        self.assertEqual(mi.vals, [tuple(insert.vals) for insert in inserts])


    def test_add_error(self):
        mi = MultiInsert("table1")
        self.assertRaises(TypeError, mi.__add__, PrimitiveBasic())
