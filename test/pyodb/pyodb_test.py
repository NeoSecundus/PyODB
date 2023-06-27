import multiprocessing
import random
import threading
from logging import Logger
from multiprocessing import Process
from test.test_models.complex_models import ComplexBasic, ComplexMulti, ComplexPydantic
from test.test_models.high_complex_models import HighComplexL3
from test.test_models.primitive_models import PrimitiveBasic, PrimitiveContainer, PrimitivePydantic
from time import sleep, time
from unittest import TestCase

from pyodb.error import BadTypeError, CacheError, PyODBError
from pyodb.pyodb import PyODB, PyODBCache


class PyODBTest(TestCase):
    def setUp(self) -> None:
        self.pyodb = PyODB(sharding=True)
        return super().setUp()


    def tearDown(self) -> None:
        if "pyodb" in vars(self):
            del self.pyodb
        return super().tearDown()


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
        self.assertNotIn(PrimitiveBasic, self.pyodb.known_types)


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
        self.pyodb.add_type(ComplexPydantic)
        self.pyodb.persistent = True
        del self.pyodb

        self.pyodb = PyODB(sharding=True)
        self.assertIn(ComplexBasic, self.pyodb._schema._tables)
        self.assertIn(PrimitiveBasic, self.pyodb._schema._tables)
        self.assertIn(PrimitiveContainer, self.pyodb._schema._tables)
        self.assertIn(ComplexPydantic, self.pyodb._schema._tables)
        self.assertIn(PrimitivePydantic, self.pyodb._schema._tables)
        del self.pyodb

        # Test Unified Schema
        self.pyodb = PyODB()
        self.pyodb.persistent = True
        self.pyodb.add_type(ComplexBasic)
        self.pyodb.add_type(ComplexPydantic)
        del self.pyodb

        self.pyodb = PyODB()
        self.assertIn(ComplexBasic, self.pyodb._schema._tables)
        self.assertIn(PrimitiveBasic, self.pyodb._schema._tables)
        self.assertIn(PrimitiveContainer, self.pyodb._schema._tables)
        self.assertIn(ComplexPydantic, self.pyodb._schema._tables)
        self.assertIn(PrimitivePydantic, self.pyodb._schema._tables)
        del self.pyodb

        self.pyodb = PyODB()
        self.assertEqual(1, len(self.pyodb._schema._tables))


    def test_expires(self):
        self.pyodb.add_type(PrimitiveBasic)
        self.pyodb.save_multiple([PrimitiveBasic() for _ in range(5)], time()+1)
        self.pyodb.save_multiple([PrimitiveBasic() for _ in range(3)], time()+5)
        sleep(2)

        self.assertEqual(len(self.pyodb.select(PrimitiveBasic).all()), 3)


    def test_highly_complex_object(self):
        self.pyodb.max_depth=5
        self.pyodb.add_type(HighComplexL3)
        hc = HighComplexL3()
        self.pyodb.save(hc)
        self.pyodb.save_multiple([HighComplexL3() for _ in range(9)])
        res = self.pyodb.select(HighComplexL3).first()
        self.assertEqual(hc, res)
        self.assertEqual(hc.high1.pyd.child.test_str, "Test Child")


class ThreadingTest(TestCase):
    def job(self, sharding: bool, pyodb: PyODB | None = None):
        random.seed = time() - int(time()-0.5)
        if pyodb is None:
            pyodb = PyODB(max_depth=3, persistent=True, sharding=sharding)
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


    def cache_job(self, sharding: bool):
        random.seed = time() - int(time()-0.5)
        cache = PyODBCache(persistent=True, sharding=sharding)
        sleep(random.random()/10 + 0.05)

        cache.add_cache("test", lambda: [ComplexBasic() for _ in range(100)], ComplexBasic, 5)
        sleep(random.random()/10 + 0.05)

        cache["test"]
        sleep(random.random()/10 + 0.05)

        cache["test"]
        sleep(random.random()/10 + 0.05)

        cache.add_cache("test2", lambda: [PrimitiveBasic() for _ in range(200)], PrimitiveBasic, 5)
        sleep(random.random()/10 + 0.05)

        cache["test2"]
        del cache


    def test_concurrency(self):
        for mode in [False, True]:
            jobs: list[threading.Thread] = []
            for i in range(8):
                sleep(0.1)
                if i == 4:
                    jobs += [
                        threading.Thread(
                            target=self.job,
                            args=[
                                mode,
                                PyODB(max_depth=3, persistent=True, sharding=mode)
                            ],
                            daemon=True
                        )
                    ]
                else:
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


    def test_multiprocessing_cache(self):
        multiprocessing.set_start_method("fork", force=True)
        for mode in [False, True]:
            jobs: list[Process] = []
            for i in range(8):
                jobs += [Process(target=self.cache_job, args=[mode], daemon= i%2 == 1)]
                jobs[-1].start()
                sleep(0.2)
                if i == 0:
                    sleep(0.6)

            for i in range(8):
                jobs[i].join()

            sleep(1.1)
            pyodb = PyODBCache(max_depth=3, sharding=mode)
            pyodb.add_cache("test", lambda: PrimitiveBasic(), PrimitiveBasic)
            pyodb.add_cache("test2", lambda: ComplexBasic(), ComplexBasic)
            self.assertEqual(len(pyodb["test"]), 100)
            self.assertEqual(len(pyodb["test2"]), 200)
            del pyodb


class PyODBCacheTest(TestCase):
    def setUp(self) -> None:
        self.cache = PyODBCache()
        self.cache.add_cache("test", lambda: [PrimitiveBasic() for _ in range(10)], PrimitiveBasic)
        return super().setUp()


    def tearDown(self) -> None:
        del self.cache
        return super().tearDown()


    def test_add_load_cache(self):
        data: list[PrimitiveBasic] = self.cache.get_data("test")
        self.assertEqual(len(data), 10)
        self.assertEqual(data[0].classmember, "cm")

        data: list[PrimitiveBasic] = self.cache.get_data("test")
        self.assertEqual(len(data), 10)
        self.assertEqual(data[0].classmember, "cm")

        before = self.cache.caches["test"].expires
        self.cache.caches["test"].expires = 0
        data: list[PrimitiveBasic] = self.cache.get_data("test")
        self.assertEqual(len(data), 10)
        self.assertEqual(data[0].classmember, "cm")
        self.assertEqual(before, self.cache.caches["test"].expires)


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


    def test_async_loading(self):
        self.cache.add_cache(
            "test",
            lambda: [PrimitiveBasic() for _ in range(100)],
            PrimitiveBasic,
            lifetime=1,
            force=True
        )
        self.assertEqual(len(self.cache["test"]), 100)
        cache2 = PyODBCache()

        cache2.add_cache(
            "test",
            lambda: [PrimitiveBasic() for _ in range(200)],
            PrimitiveBasic,
            lifetime=1
        )
        self.assertEqual(len(cache2["test"]), 100)
        sleep(1)

        self.assertEqual(len(cache2["test"]), 200)
        self.assertEqual(len(self.cache["test"]), 200)
        self.assertEqual(len(self.cache["test"]), 200)
        del cache2


    def test_cache_overwrite(self):
        self.cache.add_cache(
            "test",
            lambda: [ComplexBasic() for _ in range(10)],
            ComplexBasic,
            force=True
        )
        self.assertEqual(self.cache.caches["test"].data_type, ComplexBasic)


class ErrorTest(TestCase):
    def test_str(self):
        msg = "This is a message"
        err = PyODBError("This is a message")
        self.assertEqual(msg, str(err))
