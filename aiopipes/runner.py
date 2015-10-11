from asyncio import coroutine, iscoroutine, wait, async, FIRST_COMPLETED
from collections import Iterable
from io import TextIOBase
import inspect
import time

from aiopipes import Input, Output, IterableIO, FileIO
from .pipeio import IOFinished
from .status import StatusMixin


class Runnable(StatusMixin):
    def __init__(self, input=None, output=None):
        self.input = self._convert_to_io(input, _raise=False)
        self.output = self._convert_to_io(output)
        self.concurrency = 1
        self.started = None
        self.worker_futures = []

        super().__init__()

    @property
    def name(self):
        return self.__class__.__name__

    def log(self, msg):
        print(msg)

    @property
    def runtime(self):
        if self.started is None:
            return 0

        return time.time() - self.started

    @coroutine
    def start(self):
        @coroutine
        def runner_task():
            try:
                yield from self._run()
            except IOFinished:
                return
            except Exception as ex:
                self.status.error(ex)

        if not self.concurrency:
            # Just run a single runner_task don't bother with multiple stuff.
            fut = runner_task()
            self.worker_futures = [fut]
            try:
                yield from fut
            finally:
                if self.output:
                    yield from self.output.close()

        self.worker_futures = [async(runner_task()) for _ in range(self.concurrency)]
        self.started = time.time()

        try:
            while True:
                done, pending = yield from wait(self.worker_futures, return_when=FIRST_COMPLETED)
                self.worker_futures = list(pending)
                if not pending:
                    break
        finally:
            if self.output:
                yield from self.output.close()

    @coroutine
    def _run(self):
        raise NotImplementedError()

    def _convert_to_io(self, value, _raise=False):
        if isinstance(value, (Input, Output)):
            return value
        elif isinstance(value, TextIOBase):
            return FileIO(value)
        elif isinstance(value, Iterable):
            return IterableIO(value)
        elif _raise:
            raise RuntimeError("Cannot type {0} to input or output".format(type(value)))

    def __lt__(self, input):
        self.input = self._convert_to_io(input)
        return self

    def __gt__(self, output):
        self.output = self._convert_to_io(output)
        return self

    def parallel(self, concurrency):
        self.concurrency = concurrency
        return self


class _Continue(object):
    def __init__(self, data, done=None, max=None):
        self.data = data
        self.max = max
        self.done = done


class FunctionRunner(Runnable):
    def __init__(self, func, input=None, output=None):
        self.func = func
        super().__init__(input, output)

    @property
    def name(self):
        return self.func.__name__

    def _extract_params(self, func):
        func_args = set()

        while True:
            args = inspect.getfullargspec(func)
            for arg in args.args:
                func_args.add(arg)

            if hasattr(func, "__wrapped__"):
                func = func.__wrapped__
            else:
                break

        return func_args

    def _get_params(self, func, allowed_args=None):
        params = self._extract_params(func)
        if allowed_args is None:
            return params
        return params.intersection(allowed_args)

    def _get_param_values(self):
        @coroutine
        def _output(data):
            yield from self.output.write(data)

        return {
            "output": _output,
            "_continue": _Continue
        }

    @coroutine
    def _run(self):
        param_values = self._get_param_values()
        func_params = self._get_params(self.func, set(param_values.keys()))

        while True:
            data = yield from self.input.read()
            if data is None:
                pass

            params = {p: param_values[p] for p in func_params if p in param_values}

            with self.status.subtask("percentage_done", "done_count", "max_count") as subtask:
                subtask.percentage("done", "done_count", "max_count")

                while True:
                    try:
                        result = self.func(data, **params)
                        if iscoroutine(result):
                            result = yield from result
                        subtask.inc("done_count")

                    except Exception as ex:
                        subtask.error(ex, data)
                    else:
                        if isinstance(result, _Continue):
                            data = result.data
                            if result.max:
                                subtask.set("max_count", result.max)
                            if result.done:
                                subtask.set("done_count", result.done)
                            continue

                        if result is not None:
                            yield from self.output.write(result)

                    break
