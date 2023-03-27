import logging
import secrets
from logging import handlers


def create_logger(
        log_dir: str,
        level: int,
        console_output: bool = False
    ) -> logging.Logger:
    """Creates a logger with standard handlers and formatters"""
    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s::> %(message)s @ %(filename)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    handler = handlers.RotatingFileHandler(
        f"{log_dir}/pyodb.log",
        mode="a",
        maxBytes=500_000,
        backupCount=2,
        encoding="UTF-8"
    )
    handler.setFormatter(formatter)

    logger = logging.Logger("pyodb")
    logger.addHandler(handler)
    logger.setLevel(level)

    if console_output:
        handler2 = logging.StreamHandler()
        handler2.setFormatter(formatter)
        logger.addHandler(handler2)

    return logger


UID_CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
def generate_uid(length: int = 32) -> str:
    """Generates a UID consisting of 30 ASCII letters and digits

    Args:
        length (int): Length of the resulting UID. Defaults to 32.

    Returns:
        str: ASCII UID
    """
    return "".join([secrets.choice(UID_CHARS) for _ in range(length)])
