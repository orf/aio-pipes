from asyncio import coroutine
import json
from collections import Iterable, Sized
import io
import os

from .queue import Queue
from .status import StatusMixin

__all__ = ["Input", "Output", "FileIO", "QueueIO"]


class IOFinished(Exception):
    pass


class Input(StatusMixin):
    status_props = {"read_count", "closed", "error_count"}
    @coroutine
    def read(self):
        raise NotImplementedError()


class Output(StatusMixin):
    status_props = {"write_count", "closed", "error_count"}

    @coroutine
    def write(self, data):
        raise NotImplementedError()

    @coroutine
    def close(self):
        raise NotImplementedError()


class FileIO(Input, Output):
    status_props = {"percentage_read"}

    def __init__(self, fd: io.TextIOBase, raw_strings=False):
        self.fd = fd
        self.raw_strings = raw_strings
        super().__init__()

        if isinstance(fd, io.FileIO):
            try:
                os.path.getsize(fd.name)
            except os.error:
                pass
            else:
                self.status.percentage("read", lambda: self.fd.tell(), lambda: os.path.getsize(fd.name))

    @coroutine
    def read(self):
        line = self.fd.readline()

        if not line:
            yield from self.close()
            raise IOFinished()

        try:
            return json.loads(line)
        except Exception as e:
            self.status.error(e)
            raise IOError("Could not load JSON object")
        finally:
            self.status.inc("read_count")

    @coroutine
    def write(self, data):
        try:
            if not self.raw_strings or not isinstance(data, str):
                data = json.dumps(data)
            self.fd.write(data + "\n")
            self.status.inc("write_count")
        except Exception as e:
            self.status.error(e)
            raise IOError("Could not encode JSON object")

    @coroutine
    def close(self):
        self.status.set("closed", True)
        self.fd.close()


class QueueIO(Input, Output):
    status_props = {"queued", "percentage_done"}

    def __init__(self, queue: Queue = None):
        self.queue = queue or Queue()
        super().__init__()
        self.status.func("queued", lambda: self.queue.items_queued)
        self.status.percentage("done", "read_count", "write_count")

    @coroutine
    def write(self, data):
        self.status.inc("write_count")
        yield from self.queue.put_object(data)

    @coroutine
    def read(self):
        d = yield from self.queue.get_object()
        if d is None:
            raise IOFinished()
        self.status.inc("read_count")
        return d

    @coroutine
    def close(self):
        self.status.set("closed", True)
        return (yield from self.queue.close())


class IterableIO(Input):
    status_props = {"left_count", "percentage_done"}

    def __init__(self, it: Iterable):
        self.iterable = it
        self.it = iter(self.iterable)
        super().__init__()

        if isinstance(self.iterable, Sized):
            self.status.set("left_count", len(self.iterable))
            self.status.percentage("done", "read_count", "left_count")

    @coroutine
    def read(self):
        try:
            o = next(self.it)
            self.status.inc("read_count")
        except StopIteration:
            self.status.set("closed", True)
            raise IOFinished()

        return o

    def reset(self):
        self.it = iter(self.iterable)
        self.status.set("read_count", 0)
