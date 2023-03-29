"""Main module handling containing the main capabilities of the library.
"""
import logging
from pathlib import Path

from src.pyodb._util import create_logger
from src.pyodb.schema._shard_schema import ShardSchema
from src.pyodb.schema._unified_schema import UnifiedSchema


class PyODB:
    logger: logging.Logger | None
    _schema: ShardSchema | UnifiedSchema
    persistent: bool


    def __init__( # noqa: PLR0913
            self,
            max_depth: int = 2,
            pyodb_folder: str | Path = ".pyodb",
            persistent: bool = True,
            sharding: bool = False,
            write_log: bool = True,
            log_to_console: bool = False,
        ) -> None:
        if isinstance(pyodb_folder, str):
            pyodb_folder = Path(pyodb_folder)
        pyodb_folder.mkdir(mode=755, exist_ok=True)

        self._schema = (
            ShardSchema(pyodb_folder, max_depth)
            if sharding
            else UnifiedSchema(pyodb_folder, max_depth)
        )
        self.modify_logging(
            do_logging=write_log,
            log_folder=pyodb_folder,
            console_output=log_to_console
        )
        self.persistent = persistent


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
            self.logger = create_logger(log_folder.as_posix(), log_level, console_output)
            self._schema.logger = self.logger
        else:
            self.logger = None
            self._schema.logger = None


    @property
    def max_depth(self) -> int:
        return self._schema.max_depth


    def save_object(self, obj: object):
        obj_type = type(obj)
        if self.logger:
            self.logger.debug(f"Saving object of type {obj_type}")

        if not self._schema.is_known_type(obj_type):
            self._schema.add_type(obj_type)
        self._schema.insert(obj)
