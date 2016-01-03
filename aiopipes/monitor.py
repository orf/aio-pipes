from asyncio import coroutine, sleep

try:
    from asyncio import ensure_future
except ImportError:
    from asyncio import async as ensure_future

from .pipeline import Pipeline
from .runner import Runnable
from .pipeio import Output, FileIO
import shutil


class BaseMonitor(Runnable):
    def __init__(self, obj: Pipeline, output: Output = None):
        if not isinstance(obj, Pipeline):
            raise RuntimeError("{name} cannot monitor {o_name}".format(
                name=self.__class__.__name__,
                o_name=obj
            ))

        self.pipe = obj
        self.monitor_future = None

        super().__init__(None, output)

    @coroutine
    def monitor(self, future):
        self.monitor_future = ensure_future(future)
        yield from self.start()


    @coroutine
    def _run(self):
        while True:
            yield from sleep(1)
            if self.monitor_future.done():
                return

            try:
                pipe_stats = [
                    {
                        "input": r.input.status.get_stats(),
                        "output": r.output.status.get_stats(),
                        "task": r.status.get_stats(),
                        "name": r.name,
                        "futures": r.worker_futures
                    }
                    for r in self.pipe.pipes]

            except Exception as e:
                self.status.error(e)
                return

            monitor_info = {
                "name": self.pipe.name,
                "runtime": self.pipe.runtime,
                "pipes": pipe_stats,
                "futures": self.pipe.worker_futures
            }
            yield from self.display(monitor_info)

    @coroutine
    def display(self, info):
        raise NotImplementedError()


class ConsoleMonitor(BaseMonitor):
    def _convert_to_io(self, value, _raise=False):
        io = super()._convert_to_io(value, _raise)
        if isinstance(io, FileIO):
            io.raw_strings = True
        return io

    @coroutine
    def display(self, info):
        columns, lines = shutil.get_terminal_size()
        shutil.get_archive_formats()

        outputs = []

        def w(msg, **extra):
            outputs.append(msg.format(i=info, **extra))

        def pre(i=0):
            if i == 0:
                return " " * 10
            return "{:-<10}".format((" " * i) + "|")

        def prog(done, current=None, max=100, waiting=False):
            small_done = (int(done * 2 / 10) or 1)
            whitespace = " " * (20 - small_done)
            end = "{current:3}/{max}".format(current=current or done, max=max) if not waiting else "  -/-"
            return "[{mark:=>{small_done}}{whitespace}] {end}".format(mark="*",
                                                                      done=done,
                                                                      end=end,
                                                                      small_done=small_done,
                                                                      whitespace=whitespace)

        w("{i[name]}: {pipe_len} pipes", pipe_len=len(info["pipes"]))
        w("Runtime: {i[runtime]:.0f} seconds")

        for sub_pipe in info["pipes"]:
            errors = "[Errors: {}]".format(len(sub_pipe["task"]["error_list"]))\
                if sub_pipe["task"].get("error_list", None) else ""
            w(pre(1) + " {p[name]} {errors}", p=sub_pipe, errors=errors)
            inp = sub_pipe["input"]
            if "percentage_done" in inp:
                percent = "{inp[percentage_done][percent]:3}% done".format(inp=inp)
                current, max = inp["percentage_done"]["data"]
            elif "read_count" in inp:
                percent, current, max = "", inp["read_count"], "?"
            else:
                percent, current, max = "", "?", "?"

            w(pre() + " Input: {current}/{max} read. {percent}", inp=inp, current=current, max=max, percent=percent)
            if "percentage_done" in inp:
                w(pre() + " " + prog(inp["percentage_done"]["percent"], current, max))

            if sub_pipe["task"]["subtasks"]:
                w(pre() + " Tasks:")

            for subtask in sub_pipe["task"]["subtasks"]:
                if "percentage_done" in subtask:
                    current, max = subtask["percentage_done"]["data"]
                    w(pre(2) + " " + prog(subtask["percentage_done"]["percent"], current=current, max=max))

        yield from self.output.write("\n".join(outputs))
