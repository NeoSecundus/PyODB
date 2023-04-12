"""Main module handling containing the main capabilities of the library.
"""
import logging
from pathlib import Path
from time import time
from typing import Any, Callable

from src.pyodb._util import create_logger
from src.pyodb.error import BadTypeError, CacheError, PyODBError
from src.pyodb.schema.base._sql_builders import Delete, Select
from src.pyodb.schema.shard_schema import ShardSchema
from src.pyodb.schema.unified_schema import UnifiedSchema


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
        write_log (bool, optional): Whether to enable logging.
            Defaults to True.
        log_to_console (bool, optional): Whether to output logs in the console as well.
            Defaults to False.
        load_existing (bool, optional): Whether to load an existing schema or ignore it.
            Defaults to True.
    """
    _logger: logging.Logger | None
    _schema: ShardSchema | UnifiedSchema


    def __init__( # noqa: PLR0913
            self,
            max_depth: int = 2,
            pyodb_folder: str | Path = ".pyodb",
            persistent: bool = False,
            sharding: bool = False,
            write_log: bool = True,
            log_to_console: bool = False,
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

        self.modify_logging(
            do_logging=write_log,
            log_folder=pyodb_folder,
            console_output=log_to_console
        )


    def modify_logging(
        self,
        do_logging: bool = True,
        log_level: int = logging.WARN,
        log_folder: str | Path = ".pyodb",
        console_output: bool = False
    ):
        """Modifies the libraries logging. Log files will have a maximum of 2MiB, with 2 rotating
        files. So the logs will take up 6MiB at max.

        Args:
            log_level (int, optional): Defaults to WARN.
            log_folder (str, optional): Defaults to "./logs". Folder must exist!
            console_output (bool, optional): Whether to output logs in the console as well or only
                to the file(s). Defaults to False.
        """
        if isinstance(log_folder, str):
            log_folder = Path(log_folder)
        log_folder.mkdir(mode=755, exist_ok=True)

        if do_logging:
            self._logger = create_logger(log_folder.as_posix(), log_level, console_output)
        else:
            self._logger = None
        self._schema.logger = self._logger
        PyODBError.set_logger(self._logger)


    @property
    def max_depth(self) -> int:
        """Maximum recursion depth."""
        return self._schema.max_depth


    @max_depth.setter
    def max_depth(self, val: int):
        self._schema.max_depth = val


    def save(self, obj: object, expires: float | None = None):
        """Saves object to the database. Adds type in case it is not known.

        Args:
            obj (object): The object to save.
            expires (float | None, optional): When the object expires and gets removed from the
                database. Defaults to None.
        """
        obj_type = type(obj)
        if self._logger:
            self._logger.debug(f"Saving object of type {obj_type}")

        if not self._schema.is_known_type(obj_type):
            if self._logger:
                self._logger.info(f"Adding new type '{obj_type}'")
            self._schema.add_type(obj_type)
        self._schema.insert(obj, expires)


    @property
    def known_types(self) -> list[type]:
        return [type_ for type_ in self._schema._tables.keys() if type_.__name__ != "Table"]


    def save_multiple(self, objs: list[object], expires: float | None = None):
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


    @property
    def persistent(self) -> bool:
        """Whether the database is persistent after closing.
        In multiprocessing/threading this should always be set to true!"""
        return self._schema.is_persistent


    @persistent.setter
    def persistent(self, val: bool):
        self._schema.is_persistent = val


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
        write_log (bool, optional): Whether to enable logging.
            Defaults to True.
        log_to_console (bool, optional): Whether to output logs in the console as well.
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
            self.data = []
            self.expires = 0


        def get_data(self):
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
    def logger(self) -> logging.Logger | None:
        return self._pyodb._logger


    @property
    def pyodb(self) -> PyODB:
        """The internal pyodb instance"""
        return self._pyodb


    @property
    def caches(self) -> dict[str, _CacheItem]:
        """The currently known caches"""
        return self._caches.copy()


    def __init__( # noqa: PLR0913
            self,
            max_depth: int = 0,
            pyodb_folder: str | Path = ".pyodb",
            persistent: bool = False,
            sharding: bool = False,
            write_log: bool = True,
            log_to_console: bool = False
        ) -> None:
        self._pyodb = PyODB(
            max_depth=max_depth,
            pyodb_folder=pyodb_folder,
            persistent=persistent,
            sharding=sharding,
            write_log=write_log,
            log_to_console=log_to_console,
            load_existing=False
        )
        self._caches = {}


    def cache_exists(self, cache_key: str) -> bool:
        """Check the existance of a cache with key `cache_key`

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
            else:
                self.pyodb.remove_type(self.caches[cache_key].dataclass)

        dataclass = type(f"PyODBCache_{data_type.__name__}", (), {
                "__init__": self._dataclass_constructor,
                "__annotations__": {"data": data_type | None, "expires": float}
            }
        )
        globals().update({dataclass.__name__: dataclass})
        self.pyodb.add_type(dataclass)
        self._caches[cache_key] = self._CacheItem(data_func, data_type, lifetime, dataclass)
        if self.logger:
            self.logger.info(f"Added cache definition for '{cache_key}'")


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
            expires = time() + cache.lifetime
            self.pyodb.save_multiple([cache.dataclass(dp, expires) for dp in data], expires)
            cache.set_data(data, expires)
            if self.logger:
                self.logger.debug(f"Refreshed cached {cache_key}")
            return data
        except Exception as err:
            if self.logger:
                self.logger.error(
                    f"Failed to get/refresh datacache {cache_key}! Details: {err}"
                )
            raise err


    def __getitem__(self, key: str) -> list[Any]:
        return self.get_data(key)
