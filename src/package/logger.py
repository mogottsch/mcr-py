import logging
from logging import DEBUG, INFO, WARNING, ERROR, CRITICAL
from time import time
from rich.logging import RichHandler
import click


def setup(log_level: str):
    FORMAT = "%(message)s"
    logging.basicConfig(
        level=log_level,
        format=FORMAT,
        datefmt="[%X]",
        handlers=[RichHandler(rich_tracebacks=True, tracebacks_suppress=[click])],
    )


llog = logging.getLogger("rich")


class Timed:
    def __init__(self, level, msg, *args, **kwargs):
        self.level = level
        self.msg = msg
        self.args = args
        self.kwargs = kwargs
        self.time = time()

    def __enter__(self):
        llog.log(self.level, self.msg, *self.args, **self.kwargs)

    def __exit__(self, exc_type, exc_value, traceback):
        duration = time() - self.time
        llog.log(
            self.level,
            self.msg + f" done ({duration:.2f} seconds)",
            *self.args,
            **self.kwargs,
        )

    @staticmethod
    def debug(msg, *args, **kwargs):
        return Timed(DEBUG, msg, *args, **kwargs)

    @staticmethod
    def info(msg, *args, **kwargs):
        return Timed(INFO, msg, *args, **kwargs)

    @staticmethod
    def warning(msg, *args, **kwargs):
        return Timed(WARNING, msg, *args, **kwargs)

    @staticmethod
    def error(msg, *args, **kwargs):
        return Timed(ERROR, msg, *args, **kwargs)

    @staticmethod
    def critical(msg, *args, **kwargs):
        return Timed(CRITICAL, msg, *args, **kwargs)

    @staticmethod
    def log(level, msg, *args, **kwargs):
        return Timed(level, msg, *args, **kwargs)
