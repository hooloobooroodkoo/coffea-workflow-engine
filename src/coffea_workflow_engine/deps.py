from __future__ import annotations
from pathlib import Path
from typing import TypeVar

from .artifacts import Artifact

A = TypeVar("A", bound=Artifact)

class Deps:
    def __init__(self, executor: "Executor"):
        self._executor = executor

    def need(self, art: A) -> Path:
        # build/cache dependency and return its path on disk
        return self._executor.materialize(art)