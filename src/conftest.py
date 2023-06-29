import os

import pytest

from package.gtfs.fixtures import *
from package.structs.fixtures import *


@pytest.fixture(scope="session")
def testdata_path():
    return os.path.join(os.path.dirname(__file__), "testdata")
