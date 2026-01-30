from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Callable, Dict, Generic, Optional, Type, TypeVar

from .artifacts import Artifact

T = TypeVar("T", bound=Artifact)

ProducerFn = Callable[[T, "Deps"], Any]

_PRODUCERS: Dict[Type[Artifact], Callable[..., Any]] = {}

def producer(return_type: Type[T]) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    def deco(fn: Callable[..., Any]) -> Callable[..., Any]:
        _PRODUCERS[return_type] = fn
        return fn
    return deco

def get_producer(t: Type[T]) -> Callable[..., Any]:
    try:
        return _PRODUCERS[t]
    except KeyError:
        raise KeyError(f"No producer registered for artifact type: {t.__name__}")