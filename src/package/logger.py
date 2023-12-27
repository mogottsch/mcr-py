import io
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

nlog = logging.getLogger("null")
null_handler = logging.NullHandler()
nlog.addHandler(null_handler)


class Timer:
    def __init__(self, logger=rlog):
        self.logger = logger

    def debug(self, msg, *args, **kwargs):
        return Timed.debug(msg, self.logger, *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        return Timed.info(msg, self.logger, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        return Timed.warning(msg, self.logger, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        return Timed.error(msg, self.logger, *args, **kwargs)

    def critical(self, msg, *args, **kwargs):
        return Timed.critical(msg, self.logger, *args, **kwargs)

    def log(self, level, msg, *args, **kwargs):
        return Timed.log(level, msg, self.logger, *args, **kwargs)


class Timed:
    def __init__(self, level, msg, logger=rlog, *args, **kwargs):
        self.level = level
        self.msg = msg
        self.args = args
        self.kwargs = kwargs
        self.time = time()
        self.logger = logger

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
        self.logger.addFilter(self.f)
        self.logger.log(self.level, self.msg, *self.args, **self.kwargs)

    def __exit__(self, exc_type, exc_value, traceback):
        duration = time() - self.time
        outcome = "failed" if exc_type else "done"
        self.logger.log(
            self.level,
            self.msg + f" {outcome} ({format_duration(duration)})",
            *self.args,
            **self.kwargs,
        )
        self.logger.removeFilter(self.f)

    @staticmethod
    def debug(msg, logger=rlog, *args, **kwargs):
        return Timed(DEBUG, msg, logger, *args, **kwargs)

    @staticmethod
    def info(msg, logger=rlog, *args, **kwargs):
        return Timed(INFO, msg, logger, *args, **kwargs)

    @staticmethod
    def warning(msg, logger=rlog, *args, **kwargs):
        return Timed(WARNING, msg, logger, *args, **kwargs)

    @staticmethod
    def error(msg, logger=rlog, *args, **kwargs):
        return Timed(ERROR, msg, logger, *args, **kwargs)

    @staticmethod
    def critical(msg, logger=rlog, *args, **kwargs):
        return Timed(CRITICAL, msg, logger, *args, **kwargs)

    @staticmethod
    def log(level, msg, logger=rlog, *args, **kwargs):
        return Timed(level, msg, logger, *args, **kwargs)


def format_duration(duration: float) -> str:
    if duration < 60:
        return f"{duration:.2f} seconds"

    duration = int(duration)

    if duration < 3600:
        return f"{duration // 60}:{duration % 60:02d} minutes"
    return f"{duration // 3600}:{(duration % 3600) // 60:02d}:{duration % 60:02d} hours"


def make_string_stream_logger(name: str | None, level: int = logging.INFO):
    log_stream = io.StringIO()
    logger = logging.getLogger(name)
    logger.setLevel(level)
    handler = logging.StreamHandler(log_stream)
    handler.setLevel(level)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger, log_stream


def copy_settings_to_root_logger(logger: logging.Logger):
    root_logger = logging.getLogger()
    root_logger.setLevel(logger.level)
    root_logger.handlers = logger.handlers
    root_logger.filters = logger.filters
    root_logger.propagate = logger.propagate
