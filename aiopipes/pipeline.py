from collections import Iterable
import inspect
from asyncio import coroutine, wait

try:
    from asyncio import ensure_future
except ImportError:
    from asyncio import async as ensure_future

from . import QueueIO
from aiopipes.runner import Runnable
from .runner import FunctionRunner


class Pipeline(Runnable):
    def __init__(self, name, input=None, output=None, pipes: Iterable = None):
        self._name = name
        self.pipes = pipes or []
        super().__init__(input, output)

    @property
    def name(self):
        return self._name

    def __or__(self, other):
        if not isinstance(other, Runnable):
            if inspect.isfunction(other):
                other = FunctionRunner(other)
            else:
                raise RuntimeError("Cannot convert {type} to a pipe".format(type=type(other)))

        return self.__class__(
            self.name,
            self.input,
            self.output,
            self.pipes + [other]
        )

    @coroutine
    def _run(self):
        if not self.pipes:
            self.pipes = [FunctionRunner(lambda x: x)]  # Make the pipeline a no-op

        # Hook all our aiopipes together
        internal_ios = [QueueIO() for _ in self.pipes]

        for idx, pipe in enumerate(self.pipes):
            if idx != 0:
                pipe < internal_ios[idx]

            if idx + 1 != len(internal_ios):
                pipe > internal_ios[idx + 1]

        self.pipes[0] < self.input
        self.pipes[-1] > self.output

        self.worker_futures = [
            ensure_future(pipe.start()) for pipe in self.pipes
            ]

        yield from wait(self.worker_futures)

    def __repr__(self):
        return "<Pipeline {name}>".format(name=self.name)
