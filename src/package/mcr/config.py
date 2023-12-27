from logging import Logger
from typing import Optional
from package.logger import Timer, rlog
from package.mcr.path import PathManager


class MCRConfig:
    def __init__(
        self,
        logger: Logger = rlog,
        enable_limit: bool = False,
        disable_paths: bool = False,
    ):
        self.logger = logger
        self.timer = Timer(self.logger)
        self.path_manager: Optional[PathManager] = None
        self.enable_limit = enable_limit
        self.disable_paths = disable_paths
        if not disable_paths:
            self.path_manager = PathManager()
