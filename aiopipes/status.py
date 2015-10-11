import contextlib
import inspect
import traceback


class StatusTracker(object):
    def __init__(self, *names):
        self.parent = None

        self.percentages = {
            name.split("_", 1)[1]: {}
            for name in names
            if name.startswith("percentage_")
            }

        self.status_data = {
            name: 0 if name.endswith("_count") else None
            for name in names
            if name not in self.percentages
            }

        self.error_list = []
        self.task_list = []

    def get_stats(self):
        # Calculate percentages
        returner = {}
        for key, value in self.status_data.items():
            if isinstance(value, dict):
                value = {
                    k: v() if callable(v) else v
                    for k, v in value.items()
                    if v is not None
                }
            elif callable(value):
                value = value()
            elif not value:
                continue
            returner[key] = value

        for percentage, subdict in self.percentages.items():
            if not subdict:
                continue

            first, second = subdict["first"], subdict["second"]
            if callable(first):
                first = first()
            else:
                first = self.status_data[first]

            if callable(second):
                second = second()
            else:
                second = self.status_data[second]

            if 0 in (first, second):
                continue
            percent_done = int((int(first) / int(second)) * 100)
            if percent_done > 100:
                raise RuntimeError("You messed your percentage variables up: {0} & {1}".format(*data))
            returner["percentage_" + percentage] = {"percent": percent_done, "data": (first, second)}

        returner["subtasks"] = [t.get_stats() for t in self.task_list]
        returner["error_list"] = [e[0] for e in self.error_list]
        return returner

    @contextlib.contextmanager
    def subtask(self, *names):
        task = StatusTracker(*names)
        task.parent = self
        self.task_list.append(task)
        yield task
        self.task_list.remove(task)

    def func(self, key_name, func):
        self.status_data[key_name] = func

    def percentage(self, key, first, second):
        self.percentages[key]["first"] = first
        self.percentages[key]["second"] = second

    def set(self, key, value):
        self.status_data[key] = value

    def inc(self, key):
        self.status_data[key] += 1

    def error(self, ex, data=None):
        traceback.print_exc()
        self.error_list.append((ex, data))
        if self.parent:
            self.parent.error_list.append((ex, data))


class StatusMixin(object):
    status_props = set()

    def __init__(self):
        self.status = StatusTracker(*self.get_status_props())

    @classmethod
    def get_status_props(cls):
        props = set()

        for klass in inspect.getmro(cls):
            for prop in getattr(klass, "status_props", []):
                props.add(prop)

        return props
