from logging import Logger
from typing import Optional
from package.logger import Timer
from package.mcr.bag import IntermediateBags
from package.mcr.path import PathManager


class Step:
    def __init__(
        self,
        logger: Logger,
        timer: Timer,
        path_manager: Optional[PathManager],
        enable_limit: bool,
        disable_paths: bool,
    ):
        pass

    def run(self, input_bags: IntermediateBags, offset: int = 0) -> IntermediateBags:
        raise NotImplementedError

    def __str__(self):
        return self.__class__.__name__

    def __repr__(self):
        return str(self)


class StepBuilder:
    step = Step

    def __init__(
        self,
        **kwargs,
    ):
        self.kwargs = kwargs

    def build(
        self,
        logger: Logger,
        timer: Timer,
        path_manager: Optional[PathManager],
        enable_limit: bool,
        disable_paths: bool,
    ):
        return self.step(
            logger, timer, path_manager, enable_limit, disable_paths, **self.kwargs
        )
