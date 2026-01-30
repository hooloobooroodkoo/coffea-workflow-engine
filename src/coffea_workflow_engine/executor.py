from __future__ import annotations
from pathlib import Path
from typing import Any, Type

from .artifacts import Artifact
from .producers import get_producer
from .deps import Deps

class Executor:
    def __init__(self, cache_dir: Path):
        self.cache_dir = cache_dir

    def path_for(self, art: Artifact) -> Path:
        return self.cache_dir / art.type_name / art.identity() / "payload.json"

    def exists(self, art: Artifact) -> bool:
        return self.path_for(art).exists()

    def materialize(self, art: Artifact) -> Path:
        out = self.path_for(art)
        if out.exists():
            return out

        out.parent.mkdir(parents=True, exist_ok=True)
        fn = get_producer(type(art))
        deps = Deps(self)

        fn(target=art, deps=deps, out=out)

        if not out.exists():
            raise RuntimeError(
                f"Producer for {art.type_name} finished but did not create output at {out}"
            )
        return out

