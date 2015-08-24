from asyncio import coroutine, Future
from pipes import Input, Output

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

    def reset(self):
        if self.waiters:
            raise RuntimeError("Cannot reset while there is a waiter")

        self.__init__()
