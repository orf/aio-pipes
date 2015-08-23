from asyncio import coroutine
import json
from collections import Iterable
import io
from .queue import Queue

__all__ = ["Input", "Output", "FileIO", "QueueIO"]


class IOFinished(Exception):
    pass


class Input(object):
    @coroutine
    def read(self):
        raise NotImplementedError()


class Output(object):
    @coroutine
    def write(self, data):
        raise NotImplementedError()

    @coroutine
    def close(self):
        raise NotImplementedError()


class FileIO(Input, Output):
    def __init__(self, fd: io.TextIOBase):
        self.fd = fd

    @coroutine
    def read(self):
        return json.loads(self.fd.readline())

    @coroutine
    def write(self, data):
        self.fd.write(json.dumps(data) + "\n")

    @coroutine
    def close(self):
        self.fd.close()


class QueueIO(Input, Output):
    def __init__(self, queue: Queue=None):
        self.queue = queue or Queue()

    @coroutine
    def write(self, data):
        yield from self.queue.put_object(data)

    @coroutine
    def read(self):
        d = yield from self.queue.get_object()
        if d is None:
            raise IOFinished()
        return d

    @coroutine
    def close(self):
        return (yield from self.queue.close())


class IterableIO(Input):
    def __init__(self, it: Iterable):
        self.iterable = it
        self.it = iter(self.iterable)

    @coroutine
    def read(self):
        try:
            o = next(self.it)
        except StopIteration:
            raise IOFinished()

        return o

    def reset(self):
        self.it = iter(self.iterable)