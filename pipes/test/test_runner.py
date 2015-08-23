from ..runner import FunctionRunner


def test_get_args():
    def test(obj, output):
        pass

    runner = FunctionRunner(test)
    assert runner.params == {"output"}