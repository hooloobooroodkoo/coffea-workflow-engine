from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import coffea_workflow_engine.workflow.workflow as mdl
import coffea_workflow_engine.workflow.config as cfg
import coffea_workflow_engine.workflow.render as rnd

from coffea_workflow_engine.artifacts import CustomArtifact

from scripts.split import split_file
from scripts.count_letters import count_letters_file
from scripts.merge_counts import merge_counts


def _artifact_dir_from_payload(payload_path: str) -> Path:
    return Path(payload_path).parent


def _load_payload(payload_path: str) -> Dict[str, Any]:
    return json.loads(Path(payload_path).read_text())


def count_entrypoint(*, params: Dict[str, Any], deps: Dict[str, Any], out: Path) -> None:
    split_dep = deps["split"]
    split_payload = _load_payload(split_dep["path"])
    split_art_dir = _artifact_dir_from_payload(split_dep["path"])

    outputs = split_payload.get("outputs", {}) or {}
    chunks_dir = split_art_dir / outputs.get("chunks_dir", "chunks")

    chunk_prefix = params.get("chunk_prefix", "chunk_")
    chunk_id = params["chunk_id"]
    chunk_path = chunks_dir / f"{chunk_prefix}{chunk_id}.txt"

    out_path = out / "counts.json"
    count_letters_file(str(chunk_path), str(out_path))


def merge_entrypoint(*, params: Dict[str, Any], deps: Dict[str, Any], out: Path) -> None:
    count_deps = deps["counts"]
    count_files: List[str] = []

    for item in count_deps:
        payload_path = item["path"]
        payload = _load_payload(payload_path)
        art_dir = _artifact_dir_from_payload(payload_path)
        outputs = payload.get("outputs", {}) or {}
        counts_name = outputs.get("counts", "counts.json")
        count_files.append(str(art_dir / counts_name))

    out_file = out / "letter_counts.json"
    merge_counts(count_files, str(out_file))


def build_workflow() -> mdl.Workflow:
    workflow = mdl.Workflow()
    
    split_step = workflow.add(
        mdl.Step(
            name="split",
            step_type=CustomArtifact,
            params={
                "name": "split",
                "producer": "coffea_workflow_engine.examples.scripts.split:split_file",
                "params": {
                    "src": "lorem.txt",
                    "chunk_bytes": 200,
                    "dir": "chunks_inter_files",
                    "manifest_path": "split_manifest.json",
                },
                "outputs": {
                    "manifest": "split_manifest.json",
                    "chunks_dir": "chunks_inter_files",
                },
            },
        )
    )

    count_step = workflow.add(
        mdl.Step(
            name="count",
            step_type=CustomArtifact,
            params={
                "name": "count",
                "producer": "coffea_workflow_engine.examples.lorem_local_workflow:count_entrypoint",
                "params": {
                    "split_ref": "split",
                    "chunk_prefix": "chunk_",
                },
                "outputs": {"counts_dir": "counts_by_chunk"},
            },
        ),
        depends_on=[split_step],
    )

    merge_step = workflow.add(
        mdl.Step(
            name="merge",
            step_type=CustomArtifact,
            params={
                "name": "merge",
                "producer": "coffea_workflow_engine.examples.lorem_local_workflow:merge_entrypoint",
                "params": {
                    "counts_ref": [s.name for s in count_step],
                },
                "outputs": {"merged": "letter_counts.json"},
            },
        ),
        depends_on=count_steps,
    )

    return workflow


def main() -> None:
    workflow = build_workflow()
    config = cfg.Config(renderer="local", cache_dir=".cache")
    rnd.render(workflow, config)
    print("Successfully rendered lorem local workflow!")


if __name__ == "__main__":
    main()
