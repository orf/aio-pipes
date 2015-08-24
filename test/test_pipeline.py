from asyncio import coroutine, get_event_loop
import io

from pipes import Pipeline
from pipes.pipeio import Output, IterableIO, FileIO
from . import TestIO
import pytest


def test_pipeline_raises_runtimetimerror(pipeline, run):
    with pytest.raises(RuntimeError):
        run(pipeline.start())

        pipeline._convert_to_io(None)


def test_pipeline_concat(pipeline, test_io):
    pipe = pipeline | (lambda x: x)
    pipe < test_io

    assert isinstance(pipe, Pipeline)
    assert pipe.input is test_io
    assert pipe.output is test_io
    assert len(pipe.pipes) == 1


def test_io(pipeline):
    input, output = TestIO(), TestIO()
    pipeline < input
    pipeline > output

    assert pipeline.input is input
    assert pipeline.output is output


def test_shorthand_io(pipeline):
    input, output = range(10), io.StringIO()

    pipeline < input
    pipeline > output

    assert isinstance(pipeline.input, IterableIO)
    assert isinstance(pipeline.output, FileIO)


def test_return(pipeline, run):
    """
    Test that pipes can just return a single value instead of using an output
    """
    @coroutine
    def adder_return(input):
        return input + 1

    pipeline = pipeline | adder_return

    input = IterableIO(range(10))
    output = TestIO()

    pipeline < input
    pipeline > output

    run(pipeline.start())

    assert output.q == list(range(1, 11))


def test_pipes(pipeline, test_io, run):
    @coroutine
    def adder(input, output: Output):
        yield from output.write(input + 1)

    new_pipeline = pipeline | adder

    input = IterableIO(range(10))

    new_pipeline < input

    run(new_pipeline.start())

    assert test_io.q == [i + 1 for i in range(10)]

    # Add another adder
    new_pipeline = pipeline | adder | adder
    new_pipeline < input
    input.reset()
    test_io.reset()

    run(new_pipeline.start())

    assert test_io.q == [i + 2 for i in range(10)]

    # Use lambda
    new_pipeline = pipeline | (lambda x: x + 1)
    new_pipeline < input
    input.reset()
    test_io.reset()

    run(new_pipeline.start())

    assert test_io.q == [i + 1 for i in range(10)]
