"""Main module handling containing the main capabilities of the library.
"""
from pathlib import Path
from time import time
from typing import Any, Callable

from pyodb.error import BadTypeError, CacheError
from pyodb.schema.base._sql_builders import Delete, Select
from pyodb.schema.shard_schema import ShardSchema
from pyodb.schema.unified_schema import UnifiedSchema


class PyODB:
    """
    A persistent object database for Python based on SQLite3.

    Values of your classes can be saved by using either attribute annotations:
    ```python
    class MyClass:
        save_me: str
        _me_too: int | None
    ```

    OR a dictionary named `__odb_members__` containing the wanted attributes + types:
    ```python
    class MyClass:
        __odb_members__ = {
            "save_me": str,
            "_me_too": int | None
        }
    ```

    In case there are things that need to be done after the basic re-assembly you can define a
    method called __odb_reassemble__ which is run post assembly:

    ```python
    class MyClass:
        ...
        def __odb_reassemble__(self):
            self.was_reassembled = True
            self.some_session = NewSession()
    ```

    Args:
        max_depth (int, optional): Maximum recursion depth. Defaults to 0.
        pyodb_folder (str | Path, optional): Folder where the database is stored.
            Defaults to ".pyodb".
        persistent (bool, optional): Whether the database should be persistent after closing.
            Defaults to False.
        sharding (bool, optional): Whether to use sharding.
            Defaults to False.
        load_existing (bool, optional): Whether to load an existing schema or ignore it.
            Defaults to True.
    """
    _schema: ShardSchema | UnifiedSchema


    def __init__(
            self,
            max_depth: int = 2,
            pyodb_folder: str | Path = ".pyodb",
            persistent: bool = False,
            sharding: bool = False,
            load_existing: bool = True
        ) -> None:
        if not isinstance(pyodb_folder, Path):
            pyodb_folder = Path(pyodb_folder)
        pyodb_folder.mkdir(mode=755, exist_ok=True)

        self._schema = (
            ShardSchema(pyodb_folder, max_depth, persistent)
            if sharding
            else UnifiedSchema(pyodb_folder, max_depth, persistent)
        )
        if load_existing:
            self._schema.load_existing()


    @property
    def max_depth(self) -> int:
        """Maximum recursion depth."""
        return self._schema.max_depth


    @max_depth.setter
    def max_depth(self, val: int):
        self._schema.max_depth = val


    @property
    def persistent(self) -> bool:
        """Whether the database is persistent after closing.
        In multiprocessing/threading this should always be set to true!"""
        return self._schema.is_persistent


    @persistent.setter
    def persistent(self, val: bool):
        self._schema.is_persistent = val


    def save(self, obj: object, expires: float | None = None):
        """Saves object to the database. Adds type in case it is not known.

        Args:
            obj (object): The object to save.
            expires (float | None, optional): When the object expires and gets removed from the
                database. Defaults to None.
        """
        obj_type = type(obj)

        if not self._schema.is_known_type(obj_type):
            self._schema.add_type(obj_type)
        self._schema.insert(obj, expires)


    @property
    def known_types(self) -> list[type]:
        return [type_ for type_ in self._schema._tables.keys() if type_.__name__ != "Table"]


    def save_multiple(self, objs: list[Any], expires: float | None = None):
        """Saves multiple objects to the database. Does not add the type beforehand unlike `save`!

        Args:
            obj (list[object]): The objects to save.
            expires (float | None, optional): When the object expires and gets removed from the
                database. Defaults to None.
        """
        if not objs:
            return
        self._schema.insert_many(objs, expires)


    def remove_type(self, type_: type):
        """Completely removes a type and all sub-types from the database.

        Args:
            type_ (type): The type to remove.

        Raises:
            ParentError: Thrown in case another parent table depends on it.
        """
        self._schema.remove_type(type_)


    def add_type(self, type_: type):
        """Adds a new type to the database schema.
        Does not accept python primitives like int, float, list or dict.
        This might be necessary in some special cases or before using the `insert_multiple` method.

        Args:
            type_ (type): The type to add.
        """
        self._schema.add_type(type_)


    def select(self, type_: type) -> Select:
        """Returns a Select object for the given type. The Select object can be used to query the
        database and retrieve objects of the given type.

        Args:
            type_ (type): The type of the object to build the query for.

        Returns:
            Select: A Select object for the given type.

        Raises:
            UnknownTypeError: If the given type is not known to the schema.
        """
        return self._schema.select(type_)


    def delete(self, type_: type) -> Delete:
        """Returns a Delete object for the given type. The Delete object can be used to remove
        objects of the given type from the database.

        Args:
            type_ (type): The type of the object to build the delete for.

        Returns:
            Delete: A Delete object for the given type.

        Raises:
            UnknownTypeError: If the given type is not known to the database.
        """
        return self._schema.delete(type_)


    def clear(self):
        """Completely clears the database but keeps the table definitions."""
        self._schema.clear()


    def contains_type(self, type_: type) -> bool:
        """Check whether the given type is known by the schema.

        Args:
            type_ (type): The type to check for.

        Returns:
            bool: True if contained in schema, else False
        """
        if not isinstance(type_, type):
            raise BadTypeError("Argument 'type_' must be a non-union and non-generic type!")
        return self._schema.is_known_type(type_)


class PyODBCache:
    """Class that caches arbitrary data and returns cached data if available.
    Also updates the data if it is expired. Expiry times are set when adding a new cache.

    Args:
        max_depth (int, optional): Maximum recursion depth. Defaults to 0.
        pyodb_folder (str | Path, optional): Folder where the database is stored.
            Defaults to ".pyodb".
        persistent (bool, optional): Whether the database should be persistent after closing.
            Defaults to False.
        sharding (bool, optional): Whether to use sharding.
            Defaults to False.
    """
    class _CacheItem:
        """Cache-Definition containing the data function, the data type and the lifetime."""
        def __init__(
                self,
                data_func: Callable,
                data_type: type,
                lifetime: float,
                dataclass: type
            ) -> None:
            self.data_func = data_func
            self.data_type = data_type
            self.lifetime = lifetime
            self.dataclass = dataclass
            self.data: list = []
            self.expires: float = 0


        def get_data(self) -> list | None:
            if self.expires < time():
                self.data = []
                return None
            return self.data


        def set_data(self, data: list, expires: float):
            self.data = data
            self.expires = expires


    _caches: dict[str, _CacheItem]
    _pyodb: PyODB


    @property
    def pyodb(self) -> PyODB:
        """The internal pyodb instance"""
        return self._pyodb


    @property
    def caches(self) -> dict[str, _CacheItem]:
        """The currently known caches"""
        return self._caches.copy()


    def __init__(
            self,
            max_depth: int = 0,
            pyodb_folder: str | Path = ".pyodb",
            persistent: bool = False,
            sharding: bool = False,
        ) -> None:
        self._pyodb = PyODB(
            max_depth=max_depth,
            pyodb_folder=pyodb_folder,
            persistent=persistent,
            sharding=sharding,
            load_existing=False
        )
        self._pyodb._schema.save_table_defs = False
        self._caches = {}


    def cache_exists(self, cache_key: str) -> bool:
        """Check the existence of a cache with key `cache_key`

        Args:
            cache_key (str): Key of the cache to check for.

        Returns:
            bool: True if found, else False
        """
        return cache_key in self._caches


    @staticmethod
    def _dataclass_constructor(self_, data: Any, expires: float):
        self_.data = data
        self_.expires = expires


    def add_cache(
            self,
            cache_key: str,
            data_func: Callable,
            data_type: type,
            lifetime: float = 60,
            force: bool = False
        ):
        """Add a new cache identified by the passed cache_key. Data returned by the data_func must
        be contained in a list. (Even if it is one element only)

        Args:
            cache_key (str): Unique Key of the cache.
            data_func (Callable): A function returning a list of the specified data_type.
            data_type (type): The type which is saved by this cache and returned by the data_func.
            lifetime (float, optional): Cached data expires in `lifetime` seconds. Defaults to 60.
            force (bool, optional): Forces the cache to be overridden in case it already exists.
                Defaults to False.

        Raises:
            CacheError: Is thrown in case the cache already exists and `force` is False.
        """
        if self.cache_exists(cache_key):
            if not force:
                raise CacheError(
                    f"Cache '{cache_key}' already exists! Use 'force=True' to suppress this error."
                )
            elif data_type != self._caches[cache_key].data_type:
                    self.pyodb.remove_type(self.caches[cache_key].dataclass)
            else:
                self._caches[cache_key].data_func = data_func
                self._caches[cache_key].lifetime = lifetime
                return

        dataclass = type(f"PyODBCache_{cache_key}", (), {
                "__init__": self._dataclass_constructor,
                "__annotations__": {"data": data_type | None, "expires": float}
            }
        )
        globals().update({dataclass.__name__: dataclass})
        self.pyodb.add_type(dataclass)
        self._caches[cache_key] = self._CacheItem(data_func, data_type, lifetime, dataclass)


    def get_data(self, cache_key: str, *args, **kwargs) -> list[Any]:
        """Gets the data from the specified cache.

        Accessing data via dictionary style `cache["key"]` is also possible, as long as no arguments
        are needed for the cache's data function.

        Args:
            cache_id (str): Id of the cache to get data from.
            *args: Will be passed to the cache's data function if necessary
            **kwargs: Will be passed to the cache's data function if necessary

        Raises:
            CacheError: Cache with passed key does not exist.
            Exception: Exception thrown in data function or when trying to save the result.

        Returns:
            list[Any]: list of cached objects.
        """
        if not self.cache_exists(cache_key):
            raise CacheError(f"Cache with id '{cache_key}' does not exist!")
        cache = self._caches[cache_key]

        # Try to get data from in-memory cache
        data = cache.get_data()
        if data is not None:
            return data

        # Try to get data from database
        db_res = self.pyodb.select(cache.dataclass).all()
        data = [dp.data for dp in db_res]
        if data:
            cache.set_data(data, db_res[0].expires)
            return data

        try:
            data = cache.data_func(*args, **kwargs)
            if not data:
                return []
            expires = time() + cache.lifetime
            self.pyodb.save_multiple([cache.dataclass(dp, expires) for dp in data], expires)
            cache.set_data(data, expires)
            return data
        except Exception as err:
            raise err


    def __getitem__(self, key: str) -> list[Any]:
        return self.get_data(key)
