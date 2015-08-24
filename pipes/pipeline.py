from . import QueueIO
from pipes.runner import Runnable
from collections import Iterable
from .runner import FunctionRunner
import inspect
from asyncio import coroutine, wait


class Pipeline(Runnable):
    def __init__(self, name, input=None, output=None, pipes: Iterable=None):
        self.name = name
        self.pipes = pipes or []
        super().__init__(input, output)

    def __or__(self, other):
        if not isinstance(other, Runnable):
            if inspect.isfunction(other):
                other = FunctionRunner(other)
            else:
                raise RuntimeError("Cannot convert {type} to a pipe".format(type=type(other)))

        return Pipeline(
            self.name,
            self.input,
            self.output,
            self.pipes + [other]
        )

    @coroutine
    def _run(self):
        # Hook all our pipes together
        internal_ios = [QueueIO() for _ in self.pipes]

        for idx, pipe in enumerate(self.pipes):
            if idx != 0:
                pipe < internal_ios[idx]

            if idx + 1 != len(internal_ios):
                pipe > internal_ios[idx+1]

        self.pipes[0] < self.input
        self.pipes[-1] > self.output

        pipe_futures = [
            pipe.start() for pipe in self.pipes
        ]

        yield from wait(pipe_futures)

    def __repr__(self):
        return "<Pipeline {name}>".format(name=self.name)