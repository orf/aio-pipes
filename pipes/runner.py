from asyncio import coroutine, iscoroutine
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

    def _convert_to_io(self, value):
        if isinstance(value, (Input, Output)):
            return value
        elif isinstance(value, TextIOBase):
            return FileIO(value)
        elif isinstance(value, Iterable):
            return IterableIO(value)
        else:
            raise RuntimeError("Cannot type {0} to input or output".format(type(value)))

    def __lt__(self, input):
        self.input = self._convert_to_io(input)
        return self

    def __gt__(self, output):
        self.output = self._convert_to_io(output)
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
            try:
                data = yield from self.input.read()
            except IOError:
                # ToDo: do something clever here to stop infinite loop
                continue

            # Suppoet lambdas and other non-coroutine functions
            result = self.func(data, **{p: param_values[p] for p in self.params if p in param_values})

            if iscoroutine(result):
                result = yield from result

            if result is not None:
                yield from self.output.write(result)
