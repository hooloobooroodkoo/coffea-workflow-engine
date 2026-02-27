from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, List

import luigi
from luigi import LocalTarget

from ...artifacts import Artifact, ArtifactBase, artifact_from_dict
from ...executor import Executor
from ..config import Config
from ..workflow import Workflow
from .utils import _topo_order, _resolve_params, _print_dag

import coffea_workflow_engine.default_producers
import json

class MaterializeArtifact(luigi.Task):
    type_name = luigi.Parameter()
    artifact_id = luigi.Parameter()
    spec_dir = luigi.Parameter(default=".luigi_specs") 
    cache_dir = luigi.Parameter(default=".cache")

    def _spec_path(self) -> Path:
        return Path(self.spec_dir) / f"{self.artifact_id}.json"

    def _read_spec(self) -> dict:
        return json.loads(self._spec_path().read_text())

    def requires(self):
        spec = self._read_spec()
        reqs = []
        for dep in spec.get("deps", []):
            reqs.append(MaterializeArtifact(
                type_name=dep["type"],
                artifact_id=dep["id"],
                spec_dir=self.spec_dir,
                cache_dir=self.cache_dir,
            ))
        return reqs

    def output(self):
        # Must match your executor’s final output location for this artifact
        # Example convention:
        out_path = Path(self.cache_dir) / self.type_name / f"{self.artifact_id}.json"
        return luigi.LocalTarget(str(out_path))

    def run(self):
        spec = self._read_spec()

        # Reconstruct artifact object from spec
        # artifact = artifact_from_dict(spec["artifact"])  # your function
        # executor = Executor(cache_dir=Path(self.cache_dir))
        # executor.materialize(artifact)

        # If your executor already writes to `output().path`, great.
        # If not, make executor write to temp and rename to output().path.
        raise NotImplementedError
    

# def _serialize_param_value(value):
#     if isinstance(value, ArtifactBase):
#         return {"type": value.type_name, "keys": _serialize_param_value(value.keys())}
#     if isinstance(value, dict):
#         return {k: _serialize_param_value(v) for k, v in value.items()}
#     if isinstance(value, list):
#         return [_serialize_param_value(v) for v in value]
#     if isinstance(value, tuple):
#         return [_serialize_param_value(v) for v in value]
#     return value


# def _artifact_from_dict_recursive(d: dict) -> Artifact:
#     def resolve(value: Any) -> Any:
#         if isinstance(value, dict) and "type" in value and ("key" in value or "keys" in value):
#             return _artifact_from_dict_recursive(value)
#         if isinstance(value, dict):
#             return {k: resolve(v) for k, v in value.items()}
#         if isinstance(value, list):
#             return [resolve(v) for v in value]
#         return value

#     t = d["type"]
#     cls = __import__("coffea_workflow_engine.artifacts", fromlist=[t]).__dict__[t]
#     key = d.get("key", None)
#     if key is None:
#         key = d.get("keys", None)
#     if key is None:
#         raise ValueError("Artifact dict must include 'key' or 'keys'")
#     resolved = resolve(key)
#     return cls(**resolved)


class ArtifactTask(luigi.Task):
    step_name = luigi.Parameter()
    artifact_dict = luigi.DictParameter()
    deps = luigi.ListParameter()
    cache_dir = luigi.Parameter()

    def output(self):
        executor = Executor(cache_dir=Path(self.cache_dir))
        artifact = _artifact_from_dict_recursive(self.artifact_dict)
        return LocalTarget(str(executor.path_for(artifact)))

    def requires(self):
        tasks = []
        for dep in self.deps:
            tasks.append(
                ArtifactTask(
                    step_name=dep["name"],
                    artifact_dict=dep["artifact"],
                    deps=dep["deps"],
                    cache_dir=self.cache_dir,
                )
            )
        return tasks

    def run(self):
        executor = Executor(cache_dir=Path(self.cache_dir))
        artifact = _artifact_from_dict_recursive(self.artifact_dict)
        executor.materialize(artifact)


# def _sink_indices(num_steps: int, edges: Iterable[tuple[int, int]]) -> List[int]:
#     outgoing: Dict[int, int] = {i: 0 for i in range(num_steps)}
#     for src, dst in edges:
#         outgoing[src] += 1
#     return [i for i, out_deg in outgoing.items() if out_deg == 0]


def render_luigi(workflow: Workflow, config: Config):
    cache_dir = Path(config.cache_dir)

    if len(workflow.steps) == 0:
        return {"spec_dir": None, "root": None}

    _print_dag(workflow)
    order = _topo_order(len(workflow.steps), workflow.edges)

    # Choose a stable folder per rendered workflow
    # (you can use your own workflow hash function)
    workflow_id = "dev"  # replace with stable hash of workflow graph
    spec_dir = cache_dir / "renders" / "luigi" / workflow_id / "specs"
    spec_dir.mkdir(parents=True, exist_ok=True)

    # We'll compute artifact objects but DO NOT materialize them here.
    artifacts_by_name = {}
    id_by_name = {}
    spec_by_id = {}

    for idx in order:
        step = workflow.steps[idx]
        params = _resolve_params(step.params, artifacts_by_name)
        artifact = step.step_type(**params)

        aid = artifact.identity()  # your sha256 identity
        artifacts_by_name[step.name] = artifact
        id_by_name[step.name] = aid

        # convert dependency steps -> dependency artifact ids
        dep_ids = []
        for dep_step_idx in workflow.edges.get(idx, []):  # depends on your edge representation
            dep_step_name = workflow.steps[dep_step_idx].name
            dep_ids.append(id_by_name[dep_step_name])

        spec = {
            "id": aid,
            "type": artifact.type_name,
            "keys": artifact.keys(),
            "deps": dep_ids,
        }
        spec_by_id[aid] = spec

        (spec_dir / f"{aid}.json").write_text(json.dumps(spec, indent=2, sort_keys=True))

    # Choose a root: usually "last step in topo order" or explicit target
    root_step = workflow.steps[order[-1]].name
    root_id = id_by_name[root_step]

    render_dir = spec_dir.parent
    (render_dir / "manifest.json").write_text(json.dumps({
        "workflow_id": workflow_id,
        "root_step": root_step,
        "root_id": root_id,
        "steps": [{"name": s.name, "id": id_by_name[s.name]} for s in workflow.steps if s.name in id_by_name],
    }, indent=2, sort_keys=True))

    # optionally: write luigi_entry.py here (or keep it in your package)
    return {
        "spec_dir": str(spec_dir),
        "manifest": str(render_dir / "manifest.json"),
        "root_id": root_id,
        "root_step": root_step,
    }