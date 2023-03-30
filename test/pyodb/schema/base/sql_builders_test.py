from pathlib import Path
from unittest import TestCase
import pickle

from src.pyodb.schema.base._sql_builders import MultiInsert, Insert, Select
from test.test_models.primitive_models import PrimitiveBasic
from test.test_models.complex_models import ComplexMulti
from src.pyodb.schema._unified_schema import UnifiedSchema


class MultiInsertTest(TestCase):
    def test_add(self):
        inserts = [Insert("table1", None, None, None)]*4
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


class SelectTest(TestCase):
    def setUp(self) -> None:
        Path(".pyodb/pyodb.db").unlink(True)
        self.schema = UnifiedSchema(Path(".pyodb"), 3)
        self.schema.add_type(PrimitiveBasic)
        self.schema.add_type(ComplexMulti)
        self.pbs = [PrimitiveBasic() for _ in range(10)]
        self.cbs = [ComplexMulti() for _ in range(10)]
        self.schema.insert_many(self.pbs, None)
        self.schema.insert_many(self.cbs, None)
        return super().setUp()


    def test_eq(self):
        for i in range(10):
            query = Select(PrimitiveBasic, self.schema._tables)
            res1: PrimitiveBasic = query.eq(_private = self.pbs[i]._private).one()
            self.assertEqual(self.pbs[i], res1)

            query = Select(ComplexMulti, self.schema._tables)
            res2: ComplexMulti = query.eq(txt = self.cbs[i].txt).one()
            self.assertEqual(self.cbs[i], res2)
