from .. import Pipeline
from ..pipeio import Input, Output, IterableIO, FileIO
from asyncio import Future, coroutine, get_event_loop
import io


class TestIO(Input, Output):
    def __init__(self):
        self.q = []
        self.cursor = 0
        self.closed = False
        self.waiters = []

    @coroutine
    def write(self, data):
        self.q.append(data)

        if self.waiters:
            future = self.q[self.cursor + 1]
            future.set_result(data)
            self.cursor += 1

    @coroutine
    def read(self):
        if self.closed:
            raise RuntimeError("Read called on closed")

        if len(self.q) > self.cursor:
            future = Future()
            self.waiters.append(future)
            return future

        d = self.q[self.cursor]
        self.cursor += 1
        return d

    @coroutine
    def close(self):
        self.closed = True


def test_pipeline_concat():
    io = TestIO()

    pipe = Pipeline("Test", io, io) | (lambda x: x)

    assert isinstance(pipe, Pipeline)
    assert pipe.input is io
    assert pipe.output is io
    assert len(pipe.pipes) == 1


def test_io():
    input, output = TestIO(), TestIO()
    pipe = Pipeline("Test")
    pipe < input
    pipe > output

    assert pipe.input is input
    assert pipe.output is output


def test_shorthand_io():
    input, output = range(10), io.StringIO()

    pipe = Pipeline("Test")
    pipe < input
    pipe > output

    assert isinstance(pipe.input, IterableIO)
    assert isinstance(pipe.output, FileIO)


def test_pipes():
    @coroutine
    def adder(input, output: Output):#
        yield from output.write(input + 1)

    pipe = Pipeline("Test") | adder

    input = IterableIO(range(10))
    output = TestIO()

    pipe < input
    pipe > output

    get_event_loop().run_until_complete(
        pipe.start()
    )

    assert output.q == [i + 1 for i in range(10)]

    # Add another adder
    pipe = pipe | adder
    input.reset()
    output = TestIO()
    pipe > output

    get_event_loop().run_until_complete(
        pipe.start()
    )

    assert output.q == [i + 2 for i in range(10)]