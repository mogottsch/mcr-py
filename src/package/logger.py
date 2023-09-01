import logging
import inspect
from logging import DEBUG, INFO, WARNING, ERROR, CRITICAL
from time import time
import pathlib

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


rlog = logging.getLogger("rich")


class Timed:
    def __init__(self, level, msg, *args, **kwargs):
        self.level = level
        self.msg = msg
        self.args = args
        self.kwargs = kwargs
        self.time = time()

        # create a filter that changes the log record to point to the calling frame
        # from this file to the file that actully called Timed
        calling_frame = inspect.stack()[2].frame
        trace = inspect.getframeinfo(calling_frame)

        class UpStackFilter(logging.Filter):
            def filter(self, record):
                record.lineno = trace.lineno
                record.pathname = trace.filename
                record.filename = pathlib.Path(trace.filename).name
                return True

        self.f = UpStackFilter()

    def __enter__(self):
        rlog.addFilter(self.f)
        rlog.log(self.level, self.msg, *self.args, **self.kwargs)

    def __exit__(self, exc_type, exc_value, traceback):
        duration = time() - self.time
        outcome = "failed" if exc_type else "done"
        rlog.log(
            self.level,
            self.msg + f" {outcome} ({format_duration(duration)})",
            *self.args,
            **self.kwargs,
        )
        rlog.removeFilter(self.f)

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


def format_duration(duration: float) -> str:
    if duration < 60:
        return f"{duration:.2f} seconds"

    duration = int(duration)

    if duration < 3600:
        return f"{duration // 60}:{duration % 60:02d} minutes"
    return f"{duration // 3600}:{(duration % 3600) // 60:02d}:{duration % 60:02d} hours"
