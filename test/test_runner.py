from aiopipes.runner import FunctionRunner
import functools
from asyncio import coroutine
from decorator import decorator


def test_get_args():
    def test(obj, output):
        pass

    runner = FunctionRunner(test)
    assert runner._get_params(test, {"output"}) == {"output"}


def test_lambda_args():
    runner = FunctionRunner(lambda x: x)
    assert runner._get_params(runner.func, {}) == set()


def test_get_wrapped_args():
    @decorator
    def dec_no_args(func, *args, **kwargs):
        return func(*args, **kwargs)

    @dec_no_args
    @coroutine
    def test_function(obj, output):
        return

    runner = FunctionRunner(test_function)
    assert runner._get_params(test_function, {"output"}) == {"output"}

    @decorator
    def dec_with_args(func, *args, output, **kwargs):
        return func(*args, output=output, **kwargs)

    @dec_with_args
    @coroutine
    def test_function2(output, obj):
        return

    runner = FunctionRunner(test_function2)
    assert runner._get_params(test_function2, {"output"}) == {"output"}