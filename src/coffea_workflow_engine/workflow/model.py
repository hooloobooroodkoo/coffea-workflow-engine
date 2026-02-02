from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Type

@dataclass(frozen=True)
class Step:
    name: str
    target_type: Type 
    params: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {"name": self.name, "target_type": self.target_type.__name__, "params": self.params}

@dataclass
class Workflow:
    steps: List[Step] = field(default_factory=list)
    edges: List[Tuple[int, int]] = field(default_factory=list)

    def add(self, step: Step, depends_on: Sequence[Step] = ()) -> Step:
        self.steps.append(step)
        step_idx = len(self.steps) - 1
        dep_idxs = [self.steps.index(d) for d in depends_on]
        for di in dep_idxs:
            self.edges.append((di, step_idx))
        return step