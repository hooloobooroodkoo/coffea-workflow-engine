from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Dict, List, Sequence, Tuple, Type, Iterable

@dataclass(frozen=True)
class Step:
    name: str
    step_type: Type 
    params: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {"name": self.name, "step_type": self.step_type.__name__, "params": self.params}

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

    def add_many(self, steps: Iterable[Step], depends_on: Sequence[Step] = ()) -> List[Step]:
        """
        Add many steps that all depend on the same prerequisites.
        """
        out: List[Step] = []
        for s in steps:
            out.append(self.add(s, depends_on=depends_on))
        return out

    def add_chunk_analyses(
        self,
        *,
        name_prefix: str,
        step_type: Type,
        n_parts: int,
        common_params: Dict[str, Any],
        depends_on: Sequence[Step] = (),
        part_param: str = "part",
    ) -> List[Step]:
        """
        create N analysis steps with identical params except `part`.

        - name will be f"{name_prefix}_{part}"
        - params will be {**common_params, part_param: part}
        """
        steps = [
            Step(
                name=f"{name_prefix}_{part}",
                step_type=step_type,
                params={**common_params, part_param: part},
            )
            for part in range(n_parts)
        ]
        return self.add_many(steps, depends_on=depends_on)