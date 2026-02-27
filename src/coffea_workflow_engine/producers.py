from __future__ import annotations


_PRODUCERS = {}

def producer(return_type):
    def deco(fn):
        _PRODUCERS[return_type] = fn
        return fn
    return deco

def get_producer(t):
    try:
        return _PRODUCERS[t]
    except KeyError:
        raise KeyError(f"No producer registered for artifact type: {t.__name__}")