"""Main module handling containing the main capabilities of the library.
"""
import logging
from types import UnionType

from src.pyodb._util import create_logger
from src.pyodb.schema._base_schema import BaseSchema
from src.pyodb.schema._shard_schema import ShardSchema
from src.pyodb.schema._unified_schema import UnifiedSchema


class PyODB:
    logger: logging.Logger | None
    _max_depth: int
    _base_path: str
    _schema: BaseSchema
    persistent: bool


    def __init__( # noqa: PLR0913
            self,
            max_depth: int = 2,
            pyodb_folder: str = ".pyodb",
            persistent: bool = True,
            sharding: bool = False,
            write_log: bool = True,
            log_to_console: bool = False,
        ) -> None:
        self.change_logger(
            do_logging=write_log,
            log_folder=pyodb_folder,
            console_output=log_to_console
        )
        self._base_path = pyodb_folder
        self._max_depth = max_depth
        self._schema = (
            ShardSchema(self.logger, max_depth)
            if sharding
            else UnifiedSchema(self.logger, max_depth)
        )
        self.persistent = persistent
        self._sharding = sharding


    def change_logger(
        self,
        do_logging: bool = True,
        log_level: int = logging.WARN,
        log_folder: str = ".pyodb",
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
        if do_logging:
            self.logger = create_logger(log_folder, log_level, console_output)
        else:
            self.logger = None


    @property
    def max_depth(self) -> int:
        return self._max_depth

    @max_depth.setter
    def set_max_depth(self, depth: int):
        self._max_depth = abs(depth)


    def save_object(self, obj: object):
        obj_type = type(obj)
        if self.logger:
            self.logger.debug(f"Saving object of type {obj_type}")

        if not self._schema.is_known_type(obj_type):
            self._schema.add_type(type(obj))
        self._save_object(obj, obj_type)


    def _save_object(self, obj: object, obj_type: type | UnionType):
        pass
