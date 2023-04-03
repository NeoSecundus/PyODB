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
    _logger: logging.Logger | None
    _schema: ShardSchema | UnifiedSchema


    def __init__( # noqa: PLR0913
            self,
            max_depth: int = 1,
            pyodb_folder: str | Path = ".pyodb",
            persistent: bool = False,
            sharding: bool = False,
            write_log: bool = True,
            log_to_console: bool = False,
        ) -> None:
        if not isinstance(pyodb_folder, Path):
            pyodb_folder = Path(pyodb_folder)
        pyodb_folder.mkdir(mode=755, exist_ok=True)

        self._schema = (
            ShardSchema(pyodb_folder, max_depth, persistent)
            if sharding
            else UnifiedSchema(pyodb_folder, max_depth, persistent)
        )
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
        return self._schema.max_depth


    @max_depth.setter
    def max_depth(self, val: int):
        self._schema.max_depth = val


    def save(self, obj: object, expires: float | None = None):
        obj_type = type(obj)
        if self._logger:
            self._logger.debug(f"Saving object of type {obj_type}")

        if not self._schema.is_known_type(obj_type):
            if self._logger:
                self._logger.info(f"Adding new type '{obj_type}'")
            self._schema.add_type(obj_type)
        self._schema.insert(obj, expires)


    def save_multiple(self, obj: list[object], expires: float | None = None):
        if not obj:
            return
        self._schema.insert_many(obj, expires)


    def remove_type(self, type_: type):
        self._schema.remove_type(type_)


    def add_type(self, type_: type):
        self._schema.add_type(type_)


    def select(self, type_: type) -> Select:
        return self._schema.select(type_)


    def delete(self, type_: type) -> Delete:
        return self._schema.delete(type_)


    def clear(self):
        self._schema.clear()


    def contains_type(self, type_: type) -> bool:
        if not isinstance(type_, type):
            raise BadTypeError("Argument 'type_' must be a non-union and non-generic type!")
        return self._schema.is_known_type(type_)


    @property
    def persistent(self) -> bool:
        return self._schema.is_persistent


    @persistent.setter
    def persistent(self, val: bool):
        self._schema.is_persistent = val


class PyODBCache:
    """Class that caches database requests and returns cached data if available.
    Also updates the data if it is expired. Expiry times are set within the class."""
    class _CacheItem:
        def __init__(self, data_func: Callable, data_type: type, lifetime: float) -> None:
            self.data_func = data_func
            self.data_type = data_type
            self.lifetime = lifetime


    _caches: dict[str, _CacheItem]
    _pyodb: PyODB

    @property
    def logger(self) -> logging.Logger | None:
        return self._pyodb._logger


    @property
    def pyodb(self) -> PyODB:
        return self._pyodb


    @property
    def caches(self) -> dict[str, _CacheItem]:
        return self._caches.copy()


    def __init__(self, pyodb: PyODB) -> None:
        self._pyodb = pyodb
        self._caches = {}


    def cache_exists(self, cache_id: str) -> bool:
        return cache_id in self._caches


    def add_cache(
            self,
            cache_id: str,
            data_func: Callable,
            data_type: type,
            expiry: int = 60,
            force: bool = False
        ):
        if not force and self.cache_exists(cache_id):
            raise CacheError(
                f"Cache '{cache_id}' already exists! Use 'force=True' to suppress this error."
            )

        self.pyodb.add_type(data_type)
        self._caches[cache_id] = self._CacheItem(data_func, data_type, expiry)
        if self.logger:
            self.logger.info(f"Added cache definition for '{cache_id}'")


    def get_data(self, cache_id: str) -> list[Any]:
        if not self.cache_exists(cache_id):
            raise CacheError(f"Cache with id '{cache_id}' does not exist!")
        cache = self._caches[cache_id]

        data = self.pyodb.select(cache.data_type).all()
        if not data:
            try:
                data = cache.data_func()
                self.pyodb.save_multiple(data, time() + cache.lifetime)
                if self.logger:
                    self.logger.debug(f"Refreshed cached {cache_id}")
            except BaseException as err:
                if self.logger:
                    self.logger.error(f"Failed to get/refresh datacache {cache_id}! Details: {err}")
                raise err

        return data
