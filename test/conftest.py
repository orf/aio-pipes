import pytest
from pipes import Pipeline
from . import TestIO
from asyncio import get_event_loop


@pytest.fixture
def run():
    return get_event_loop().run_until_complete


@pytest.fixture
def pipeline():
    return Pipeline("Test")


@pytest.fixture
def test_io(pipeline):
    io = TestIO()
    pipeline > io
    return io


