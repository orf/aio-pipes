from asyncio import coroutine
from pipes import Input, Output, IterableIO, FileIO
from collections import Iterable
from io import TextIOBase
from .pipeio import IOFinished
import inspect


class Runnable(object):
    def __init__(self, input: Input=None, output: Output=None):
        self.input = input
        self.output = output

    @coroutine
    def start(self):
        if self.input is None or self.output is None:
            raise RuntimeError("Cannot start pipeline without an input or output")

        try:
            yield from self._run()
        except IOFinished:
            yield from self.output.close()

    @coroutine
    def _run(self):
        raise NotImplementedError()

    def __lt__(self, input):
        if isinstance(input, Input):
            self.input = input
        elif isinstance(input, Iterable):
            self.input = IterableIO(input)
        else:
            raise RuntimeError("Cannot input {0}".format(type(input)))

        return self

    def __gt__(self, output):
        if isinstance(output, Output):
            self.output = output
        elif isinstance(output, TextIOBase):
            self.output = FileIO(output)
        else:
            raise RuntimeError("Cannot input {0}".format(type(input)))
        return self


class FunctionRunner(Runnable):
    def __init__(self, func, input=None, output=None):
        self.func = func
        self.params = self._get_params()

        super().__init__(input, output)

    def _get_params(self):
        func_args, allowed_args = set(), {"output"}
        func = self.func

        while True:
            args = inspect.getfullargspec(func)
            for arg in args.args:
                if arg in allowed_args:
                    func_args.add(arg)

            if hasattr(func, "__wrapped__"):
                func = func.__wrapped__
            else:
                break

        return func_args

    @coroutine
    def _run(self):
        param_values = {
            "output": self.output
        }

        while True:
            data = yield from self.input.read()
            result = yield from self.func(data, **{p: param_values[p] for p in self.params if p in param_values})

            self.output.write(result)
