"""Main module handling containing the main capabilities of the library.
"""
import logging
from typing import Any

import src.pyodb._assembly as assembly
import src.pyodb._disassembly as disassembly
from src.pyodb._util import create_logger
from src.pyodb.schema._table_schema import TableSchema


class PyODB:
    _logger: logging.Logger = None
    _max_depth: int = 5

    @classmethod
    def activate_logging(
        cls,
        log_level: int = logging.WARN,
        log_folder: str = "./logs",
        console_output: bool = False
    ):
        """Activates the libraries logging. Log files will have a maximum of 2MiB, with 2 rotating
        files. So the logs will take up 6MiB at max.

        Args:
            log_level (int, optional): Defaults to WARN.
            log_folder (str, optional): Defaults to "./logs". Folder must exist!
            console_output (bool, optional): Whether to output logs in the console as well or only
                to the file(s). Defaults to False.
        """
        cls._logger = create_logger(log_folder, log_level, console_output)


    @property
    @classmethod
    def max_depth(cls) -> int:
        return cls._max_depth

    @max_depth.setter
    @classmethod
    def set_max_depth(cls, depth: int) -> int:
        cls._max_depth = abs(depth)


    @classmethod
    def save_object(cls, obj: object):
        obj_type = type(obj)
        if cls._logger:
            cls._logger.debug(f"Saving object of type {obj_type}")

        if TableSchema.is_known_type(obj_type):
            TableSchema.save(obj, obj_type)
            return

        disassembly.unpack_object(obj, max_depth=cls._max_depth)


    @classmethod
    def load_object(cls, obj: object) -> Any:
        obj_type = type(obj)
        if not TableSchema.is_known_type(obj_type):
            raise TypeError(f"Type '{obj_type}' does not have a Schema definition yet!")

        obj = assembly.assemble_object(obj, obj_type)
        return obj
