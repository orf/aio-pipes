from asyncio import coroutine, Future, Queue as ioQueue


class QueueDone(Exception):
    def __init__(self, future: Future):
        self.future = future


class Queue(object):
    def __init__(self, queue: ioQueue=None):
        self._queue = queue or ioQueue()
        self.finished = False

    @coroutine
    def get_object(self):
        obj = yield from self._queue.get()

        if isinstance(obj, QueueDone):
            obj.future.set_result(True)
            self.finished = True
        else:
            return obj

    @coroutine
    def put_object(self, object):
        yield from self._queue.put(object)


    @coroutine
    def close(self):
        future = Future()

        yield from self.put_object(
            QueueDone(future)
        )

        return future