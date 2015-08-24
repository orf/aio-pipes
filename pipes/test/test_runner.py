from ..runner import FunctionRunner
import functools
from asyncio import coroutine
from decorator import decorator


def test_get_args():
    def test(obj, output):
        pass

    runner = FunctionRunner(test)
    assert runner.params == {"output"}


def test_get_wrapped_args():
    @decorator
    def dec_no_args(func, *args, **kwargs):
        return func(*args, **kwargs)

    @dec_no_args
    @coroutine
    def test_function(obj, output):
        return

    runner = FunctionRunner(test_function)
    assert runner.params == {"output"}

    @decorator
    def dec_with_args(func, *args, output, **kwargs):
        return func(*args, output=output, **kwargs)

    @dec_with_args
    @coroutine
    def test_function2(output, obj):
        return

    runner = FunctionRunner(test_function2)
    assert runner.params == {"output"}