from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, List

from ...artifacts import Artifact, artifact_from_dict
from ...executor import Executor
from ..config import Config
from ..workflow import Workflow


import coffea_workflow_engine.default_producers


def _topo_order(num_steps: int, edges: Iterable[tuple[int, int]]) -> List[int]:
    outgoing: Dict[int, List[int]] = {i: [] for i in range(num_steps)}
    in_deg = {i: 0 for i in range(num_steps)}
    for src, dst in edges:
        outgoing[src].append(dst)
        in_deg[dst] += 1

    queue = [i for i in range(num_steps) if in_deg[i] == 0]
    order: List[int] = []
    while queue:
        idx = queue.pop(0)
        order.append(idx)
        for nxt in outgoing[idx]:
            in_deg[nxt] -= 1
            if in_deg[nxt] == 0:
                queue.append(nxt)

    if len(order) != num_steps:
        raise ValueError("Workflow has a cycle or disconnected dependency graph")
    return order


def _resolve_params(
    raw_params: Dict[str, Any],
    artifacts_by_name: Dict[str, Artifact],
) -> Dict[str, Any]:
    resolved: Dict[str, Any] = {}
    for key, value in raw_params.items():
        target_key = key[:-4] if key.endswith("_ref") else key

        if isinstance(value, dict) and "type" in value and ("key" in value or "keys" in value):
            resolved[target_key] = artifact_from_dict(value)
            continue

        if key.endswith("_ref") and isinstance(value, str) and value in artifacts_by_name:
            resolved[target_key] = artifacts_by_name[value]
            continue

        resolved[target_key] = value
    return resolved


def _print_dag(workflow: Workflow) -> None:
    print("Workflow DAG:")
    if not workflow.steps:
        print("  (no steps)")
        return
    for idx, step in enumerate(workflow.steps):
        print(
            f"  [{idx}] {step.name} -> {step.step_type.__name__} params={step.params}"
        )
    if workflow.edges:
        print("Edges:")
        for src, dst in workflow.edges:
            print(f"  {workflow.steps[src].name} -> {workflow.steps[dst].name}")
    else:
        print("Edges: (none)")


def render_local(workflow: Workflow, config: Config) -> Dict[str, Any]:
    cache_dir = Path(config.cache_dir)
    executor = Executor(cache_dir=cache_dir)

    num_steps = len(workflow.steps)
    if num_steps == 0:
        return {"paths": {}, "artifacts": {}, "order": []}

    _print_dag(workflow)
    order = _topo_order(num_steps, workflow.edges)
    artifacts_by_name: Dict[str, Artifact] = {}
    paths_by_name: Dict[str, Path] = {}

    for idx in order:
        step = workflow.steps[idx]
        params = _resolve_params(step.params, artifacts_by_name)
        artifact = step.step_type(**params)
        print(
            f"Executing step '{step.name}': "
            f"{artifact.type_name} params={artifact.keys()}"
        )
        path = executor.materialize(artifact)
        print(f"  -> materialized at {path}")
        artifacts_by_name[step.name] = artifact
        paths_by_name[step.name] = path

    return {"paths": paths_by_name, "artifacts": artifacts_by_name, "order": [workflow.steps[i].name for i in order]}
