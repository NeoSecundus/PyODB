from pathlib import Path
from test.test_models.complex_models import ComplexMulti
from test.test_models.primitive_models import PrimitiveBasic, PrimitiveContainer
from unittest import TestCase

from src.pyodb.schema._unified_schema import UnifiedSchema
from src.pyodb.schema.base._sql_builders import Delete, Insert, MultiInsert, Select


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
        self.schema = UnifiedSchema(Path(".pyodb"), 3, False)
        self.schema.add_type(PrimitiveBasic)
        self.schema.add_type(ComplexMulti)
        self.pbs = [PrimitiveBasic() for _ in range(10)]
        self.cbs = [ComplexMulti() for _ in range(10)]
        self.schema.insert_many(self.pbs, None)
        self.schema.insert_many(self.cbs, None)
        return super().setUp()


    def tearDown(self) -> None:
        del self.schema
        return super().tearDown()


    def test_eq(self):
        for i in range(10):
            query = Select(PrimitiveBasic, self.schema._tables)
            res1: PrimitiveBasic = query.eq(_private = self.pbs[i]._private).one()
            self.assertEqual(self.pbs[i], res1)

            query = Select(ComplexMulti, self.schema._tables)
            res2: ComplexMulti = query.eq(txt = self.cbs[i].txt).one()
            self.assertEqual(self.cbs[i], res2)

            query.eq(txt = None)

        select = Select(ComplexMulti, self.schema._tables)
        self.assertRaises(TypeError, select.eq, _private = self.pbs[0])


    def test_ne(self):
        for i in range(1,10):
            query = Select(PrimitiveBasic, self.schema._tables)
            res1: list[PrimitiveBasic] = query.ne(_private = self.pbs[i]._private).all()
            self.assertNotIn(self.pbs[i], res1)
            self.assertIn(self.pbs[i-1], res1)

            query = Select(ComplexMulti, self.schema._tables)
            res2: list[ComplexMulti] = query.ne(txt = self.cbs[i].txt).all()
            self.assertNotIn(self.cbs[i], res2)
            self.assertIn(self.cbs[i-1], res2)

            query.ne(txt = None)

        select = Select(ComplexMulti, self.schema._tables)
        self.assertRaises(TypeError, select.ne, _private = self.pbs[0])


    def test_lt_le(self):
        for i in range(10):
            query = Select(PrimitiveBasic, self.schema._tables)
            res1: list[PrimitiveBasic] = query.le(_private = self.pbs[i]._private).all()
            lr1 = len(res1)
            self.assertIn(self.pbs[i], res1)
            self.assertGreater(len(res1), 0)

            res1: list[PrimitiveBasic] = query.lt(_private = self.pbs[i]._private).all()
            self.assertNotIn(self.pbs[i], res1)
            self.assertEqual(len(res1), lr1-1)

        select = Select(ComplexMulti, self.schema._tables)
        self.assertRaises(TypeError, select.lt, _private = self.pbs[0])
        self.assertRaises(TypeError, select.le, _private = self.pbs[0])


    def test_gt_ge(self):
        for i in range(10):
            query = Select(PrimitiveBasic, self.schema._tables)
            res1: list[PrimitiveBasic] = query.gt(_private = self.pbs[i]._private).all()
            lr1 = len(res1)
            self.assertNotIn(self.pbs[i], res1)

            res1: list[PrimitiveBasic] = query.ge(_private = self.pbs[i]._private).all()
            self.assertIn(self.pbs[i], res1)
            self.assertEqual(len(res1), lr1+1)

        select = Select(ComplexMulti, self.schema._tables)
        self.assertRaises(TypeError, select.gt, _private = self.pbs[0])
        self.assertRaises(TypeError, select.ge, _private = self.pbs[0])


    def test_like_nlike(self):
        for i in range(10):
            pattern = self.pbs[i].text[:len(self.pbs[i].text)-1] + "_"
            query = Select(PrimitiveBasic, self.schema._tables)
            res1: PrimitiveBasic = query.like(text = pattern).one()
            self.assertEqual(self.pbs[i], res1)

            res2: list[PrimitiveBasic] = query.nlike(text = pattern).all()
            self.assertNotIn(self.pbs[i], res2)
            self.assertGreaterEqual(len(res2), 9)

        select = Select(ComplexMulti, self.schema._tables)
        self.assertRaises(TypeError, select.like, text = 8)
        self.assertRaises(TypeError, select.nlike, text = True)


    def test_limit_offset(self):
        query = Select(PrimitiveBasic, self.schema._tables).limit(3, 4)
        res: list[PrimitiveBasic] = query.all()
        self.assertEqual(len(res), 3)
        self.assertEqual(res[0], self.pbs[4])
        self.assertRaises(ValueError, query.limit, 0)
        self.assertRaises(ValueError, query.limit, 1, -1)


    def test_compile_one_errors(self):
        query = Select(PrimitiveBasic, self.schema._tables).limit(2)
        self.assertRaises(IndexError, query.one)
        self.assertRaises(IndexError, query.eq(text = None).one)
        query._table.dbconn = None
        self.assertRaises(ConnectionError, query.first)


    def test_first(self):
        query = Select(PrimitiveBasic, self.schema._tables).limit(2)
        res = query.first()
        self.assertEqual(type(res), PrimitiveBasic)
        self.assertEqual(res, self.pbs[0])


    def test_count(self):
        res = Select(ComplexMulti, self.schema._tables).count()
        self.assertEqual(res, 10)


class DeleteTest(TestCase):
    def setUp(self) -> None:
        self.schema = UnifiedSchema(Path(".pyodb"), 3, False)
        self.schema.add_type(PrimitiveBasic)
        self.schema.add_type(ComplexMulti)
        self.pbs = [PrimitiveBasic() for _ in range(10)]
        self.cbs = [ComplexMulti() for _ in range(10)]
        self.schema.insert_many(self.pbs, None)
        self.schema.insert_many(self.cbs, None)
        return super().setUp()


    def tearDown(self) -> None:
        del self.schema
        return super().tearDown()


    def test_delete_count(self):
        query = Delete(ComplexMulti, self.schema._tables)
        res = query.commit()
        self.assertEqual(res, 10)

        res = Select(PrimitiveBasic, self.schema._tables).count()
        self.assertGreater(res, 0)
        res = Select(ComplexMulti, self.schema._tables).count()
        self.assertEqual(res, 0)


    def test_delete_full_count(self):
        query = Delete(ComplexMulti, self.schema._tables)
        res = query.commit(True)
        self.assertEqual(res, 20)


    def test_throw_subtype_error(self):
        table = self.schema._tables[ComplexMulti]
        if table.dbconn:
            table.dbconn.execute(
                f"UPDATE \"{table.fqcn}\" SET multi = 'test.test_models.complex_models.ComplexContainer';"
            )
            table.dbconn.commit()

        query = Delete(ComplexMulti, self.schema._tables)
        self.assertRaises(TypeError, query.commit)


    def test_connection_error(self):
        self.schema._tables[ComplexMulti].dbconn = None
        query = Delete(ComplexMulti, self.schema._tables)
        self.assertRaises(ConnectionError, query.commit)


    def test_non_parent_table_error(self):
        query = Delete(PrimitiveContainer, self.schema._tables)
        self.assertRaises(TypeError, query.commit)
