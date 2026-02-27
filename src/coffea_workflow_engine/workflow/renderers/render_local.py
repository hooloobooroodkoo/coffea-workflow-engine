from __future__ import annotations

from pathlib import Path
from typing import Dict

from ...artifacts import Artifact, artifact_from_dict
from ...executor import Executor
from ..config import Config
from ..workflow import Workflow
from .utils import _topo_order, _resolve_params, _print_dag




import coffea_workflow_engine.default_producers


def render_local(workflow: Workflow, config: Config):
    cache_dir = Path(config.cache_dir)
    executor = Executor(cache_dir=cache_dir)

    num_steps = len(workflow.steps)
    if num_steps == 0:
        return {"paths": {}, "artifacts": {}, "order": []}

    _print_dag(workflow)
    
    order = _topo_order(num_steps, workflow.edges)
    artifacts_by_name = {}
    paths_by_name = {}

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
