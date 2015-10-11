import collections
import functools


def unique(key):
    seen_keys = set()
    @functools.wraps(unique)
    def _inner(item):
        if isinstance(item, collections.Mapping):
            key_value = item[key]
        else:
            key_value = getattr(item, key)

        if key_value in seen_keys:
            return None

        seen_keys.add(key_value)
        return item

    return _inner
