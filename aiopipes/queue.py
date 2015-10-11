from asyncio import coroutine, Future, Queue as ioQueue


class QueueDone(Exception):
    def __init__(self, future: Future):
        self.future = future


class Queue(object):
    def __init__(self, queue: ioQueue=None):
        self._queue = queue or ioQueue()
        self.finished = False

    @property
    def items_queued(self):
        return self._queue.qsize()

    @coroutine
    def get_object(self):
        if self.finished:
            return

        obj = yield from self._queue.get()

        if isinstance(obj, QueueDone):
            for getter in self._queue._getters:
                getter.set_result(None)

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