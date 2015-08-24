import pytest
from pipes import Pipeline
from . import TestIO


@pytest.fixture
def pipeline():
    return Pipeline("Test")


@pytest.fixture
def test_io(pipeline):
    io = TestIO()
    pipeline > io
    return io


