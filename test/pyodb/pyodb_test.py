import multiprocessing
import random
import threading
from logging import Logger
from multiprocessing import Process
from test.test_models.complex_models import ComplexBasic, ComplexMulti
from test.test_models.high_complex_models import HighComplexL3
from test.test_models.primitive_models import PrimitiveBasic, PrimitiveContainer
from time import sleep, time
from unittest import TestCase

from src.pyodb.error import BadTypeError, CacheError
from src.pyodb.pyodb import PyODB, PyODBCache


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
        self.assertRaises(BadTypeError, self.pyodb.contains_type, ComplexMulti())


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


    def test_expires(self):
        self.pyodb.add_type(PrimitiveBasic)
        self.pyodb.save_multiple([PrimitiveBasic() for _ in range(5)], time()+1)
        self.pyodb.save_multiple([PrimitiveBasic() for _ in range(3)], time()+5)
        sleep(2)

        self.assertEqual(len(self.pyodb.select(PrimitiveBasic).all()), 3)


    def test_highly_complex_object(self):
        self.pyodb.max_depth=2
        self.pyodb.add_type(HighComplexL3)
        hc = HighComplexL3()
        self.pyodb.save(hc)
        self.pyodb.save_multiple([HighComplexL3() for _ in range(9)])
        res = self.pyodb.select(HighComplexL3).first()
        self.assertEqual(hc, res)


class ThreadingTest(TestCase):
    def job(self, sharding: bool):
        random.seed = time() - int(time()-0.5)
        pyodb = PyODB(max_depth=3, persistent=True, sharding=sharding, log_to_console=True)
        sleep(random.random()/10 + 0.05)

        pyodb.add_type(PrimitiveBasic)
        sleep(random.random()/10 + 0.05)

        pyodb.add_type(ComplexBasic)
        sleep(random.random()/10 + 0.05)

        pyodb.save(ComplexBasic(), time()+1)
        sleep(random.random()/10 + 0.05)

        pyodb.save_multiple([PrimitiveBasic(), PrimitiveBasic()])
        sleep(random.random()/10 + 0.05)

        pyodb.select(ComplexBasic).all()
        del pyodb


    def test_concurrency(self):
        for mode in [False, True]:
            jobs: list[threading.Thread] = []
            for i in range(8):
                sleep(0.1)
                jobs += [threading.Thread(target=self.job, args=[mode], daemon= i%2 == 1)]
                jobs[-1].start()

            for i in range(8):
                jobs[i].join()

            pyodb = PyODB(max_depth=3, sharding=mode)
            sleep(1.1)
            self.assertEqual(pyodb.select(PrimitiveBasic).count(), 16)
            self.assertEqual(pyodb.select(ComplexBasic).count(), 0)
            del pyodb


    def test_multiprocessing(self):
        multiprocessing.set_start_method("fork")
        for mode in [False, True]:
            jobs: list[Process] = []
            for i in range(8):
                sleep(0.1)
                jobs += [Process(target=self.job, args=[mode], daemon= i%2 == 1)]
                jobs[-1].start()

            for i in range(8):
                jobs[i].join()

            sleep(1.1)
            pyodb = PyODB(max_depth=3, sharding=mode)
            self.assertEqual(pyodb.select(PrimitiveBasic).count(), 16)
            self.assertEqual(pyodb.select(ComplexBasic).count(), 0)
            del pyodb


class PyODBCacheTest(TestCase):
    def setUp(self) -> None:
        self.cache = PyODBCache(PyODB())
        self.cache.add_cache("test", lambda: [PrimitiveBasic() for _ in range(10)], PrimitiveBasic)
        return super().setUp()


    def tearDown(self) -> None:
        del self.cache
        return super().tearDown()


    def test_add_load_cache(self):
        data: list[PrimitiveBasic] = self.cache.get_data("test")

        self.assertEqual(len(data), 10)
        self.assertEqual(data[0].classmember, "cm")


    def test_get_caches(self):
        ccs = self.cache.caches
        self.assertEqual(ccs["test"].data_type, PrimitiveBasic)
        self.assertEqual(ccs["test"].lifetime, 60)


    def test_cache_exists_error(self):
        self.assertRaises(
            CacheError,
            self.cache.add_cache,
            "test",
            lambda: [PrimitiveBasic() for _ in range(10)],
            PrimitiveBasic
        )

    def test_cache_not_exists_error(self):
        self.assertRaises(
            CacheError,
            self.cache.get_data,
            "bad_cache"
        )


    def error(self):
        raise ValueError("Some arbitrary error")


    def test_data_func_error(self):
        self.cache.add_cache(
            "error",
            self.error,
            PrimitiveBasic
        )
        self.assertRaises(ValueError, self.cache.get_data, "error")
