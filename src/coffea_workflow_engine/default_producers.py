# src/coffea_workflow_engine/default_producers.py
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Dict, List, Any
import importlib
import inspect

import cloudpickle

from .artifacts import Fileset, Chunking, ChunkAnalysis, MergedResult, Plots
from .deps import Deps
from .producers import producer


def _load_fileset_source() -> Dict[str, List[str]]:
    """
    load dataset->files list from a JSON file.
    The path is given by COFFEA_FILESET_JSON env var, otherwise ./filesets.json.

    Example filesets.json:
    {
      "TTbar:2018": ["root://.../file1.root", "root://.../file2.root"],
      "DataMuon:2018": [...]
    }
    """
    env_path = os.environ.get("COFFEA_FILESET_JSON")
    if env_path:
        p = Path(env_path)
    else:
        cwd_default = Path("filesets.json")
        package_default = Path(__file__).with_name("filesets.json")
        p = cwd_default if cwd_default.exists() else package_default

    if not p.exists():
        raise FileNotFoundError(
            f"Missing fileset source JSON: {p}. "
            f"Set COFFEA_FILESET_JSON or create filesets.json."
        )
    with p.open() as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise TypeError("filesets.json must contain a JSON object mapping dataset keys to file lists")
    return data


@producer(Fileset)
def make_fileset(*, target: Fileset, deps, out: Path) -> None:
    """
    Writes a fileset manifest to `out`.
    """
    source = _load_fileset_source()
    key = f"{target.dataset}:{target.era}"
    files = source.get(key)
    if files is None:
        raise KeyError(f"Dataset key '{key}' not found in fileset source JSON")
    if not isinstance(files, list):
        raise TypeError(f"Fileset entry for '{key}' must be a list")

    payload = {
        "dataset": target.dataset,
        "era": target.era,
        "files": files,
    }
    out.write_text(json.dumps(payload, indent=2))


@producer(Chunking)
def make_partition(*, target: Chunking, deps, out: Path) -> None:
    """
    Chunks a Fileset manifest into N parts and write partition manifest.
    """
    fileset_path = deps.need(target.fileset)
    fileset = json.loads(fileset_path.read_text())

    files = fileset["files"]
    n_parts = target.n_parts
    if n_parts <= 0:
        raise ValueError("n_parts must be > 0")
    if not files:
        raise ValueError("Fileset has 0 files; nothing to partition")

    # simple partitioning
    parts = [[] for _ in range(n_parts)]
    for i, f in enumerate(files):
        parts[i % n_parts].append(f)

    manifest = {
        "dataset": fileset["dataset"],
        "era": fileset["era"],
        "n_parts": n_parts,
        "parts": [
            {"part": i, "files": part_files}
            for i, part_files in enumerate(parts)
            if part_files  # drop empty parts (useful if n_parts > n_files)
        ],
    }
    out.write_text(json.dumps(manifest, indent=2))

def _fileset_from_list_payload(fileset_payload: Dict[str, Any], files: List[str]) -> Dict[str, Any]:
    return {
        f"{fileset_payload['dataset']}__{fileset_payload['era']}": {
            "files": files,
            "metadata": {
                "dataset_name": fileset_payload["dataset"],
                "era": fileset_payload["era"],
            },
        }
    }

def _load_object(path: str) -> Any:
    """
    Initiate an object
    """
    if ":" in path:
        mod_name, attr = path.split(":", 1)
    else:
        mod_name, attr = path.rsplit(".", 1)
    module = importlib.import_module(mod_name)
    try:
        return getattr(module, attr)
    except AttributeError as e:
        raise AttributeError(f"Object '{attr}' not found in module '{mod_name}'") from e

def _resolve_executor(executor: Optional[str], executor_params: Dict[str, Any]):
    """
    TODO: implement support of  other executors. 
    """
    if executor not in (None, "futures"):
        raise ValueError(
            f"Only executor='futures' is supported (got {executor!r})."
        )

    from coffea.processor.executor import FuturesExecutor

    workers = int(executor_params.get("workers", 1))
    try:
        return FuturesExecutor(workers=workers)
    except TypeError:
        return FuturesExecutor(max_workers=workers)
    
@producer(ChunkAnalysis)
def make_chunk_analysis(*, target: ChunkAnalysis, deps: Deps, out: Path) -> None:
    """
    Runs a Coffea processor on one partition from a Chunking manifest.

    Expects ChunkAnalysis to carry:
      - chunking: Chunking
      - part: int
      - tag: str (optional but recommended)
      - processor: str|callable|instance
      - processor_params: dict
      - executor: str|callable|...
      - executor_params: dict
      - treename: str
    """
    from coffea.nanoevents import NanoAODSchema
    from coffea.processor.executor import Runner
    from coffea.processor import ProcessorABC

    chunk_path = deps.need(target.chunk)
    chunk_payload = json.loads(chunk_path.read_text())
    files = chunk_payload.get("files", [])
    if not isinstance(files, list) or not files:
        raise ValueError("ChunkAnalysis requires Chunking with non-empty 'files'")

    fileset = _fileset_from_list_payload(chunk_payload, files)
    processor_obj = _load_object(target.processor) if isinstance(target.processor, str) else target.processor
    
    if isinstance(processor_obj, ProcessorABC):
        processor_instance = processor_obj
    elif inspect.isclass(processor_obj) or callable(processor_obj):
        processor_instance = _call_with_accepted_kwargs(processor_obj, target.processor_params)
    else:
        raise TypeError("processor must be a ProcessorABC instance, class, or factory")

    executor_params = {
        "schema": NanoAODSchema,
        **(target.executor_params or {}),
    }
    schema = executor_params.get("schema")
    if isinstance(schema, str):
        executor_params["schema"] = _load_object(schema)

    executor = _resolve_executor(target.executor, executor_params)
    runner = Runner(
        executor=executor,
        schema=executor_params.get("schema", NanoAODSchema),
        chunksize=executor_params.get("chunksize", 200_000),
        savemetrics=executor_params.get("savemetrics", True),
        metadata_cache=executor_params.get("metadata_cache", {}),
    )

    output = runner(
        fileset=fileset,
        treename=target.treename,
        processor_instance=processor_instance,
    )

    if isinstance(output, tuple) and len(output) >= 1:
        output = output[0]

    payload_path = out.parent / "payload.pkl"
    with payload_path.open("wb") as f:
        cloudpickle.dump(output, f)

    summary = {"nevents": output.get("nevents")} if isinstance(output, dict) else {}
    out.write_text(
        json.dumps(
            {
                "payload": payload_path.name,
                "summary": summary,
                "chunk_files": files,
                "parameters": target.keys(),
            },
            indent=2,
        )
    )



def _scan_chunk_results(cache_root: Path, dataset: str, era: str, tag: str | None) -> List[Path]:
    chunk_dir = cache_root / "ChunkAnalysis"
    if not chunk_dir.exists():
        return []

    results: List[Path] = []
    for result_path in chunk_dir.rglob("payload.json"):
        try:
            payload = json.loads(result_path.read_text())
        except json.JSONDecodeError:
            continue
        if (
            payload.get("dataset") == dataset
            and payload.get("era") == era
            and payload.get("tag") == tag
        ):
            results.append(payload)
    return results


@producer(MergedResult)
def make_merged_result(*, target: MergedResult, deps: Deps, out: Path) -> None:
    """
    Merge available ChunkAnalysis outputs for the same dataset/era/(tag), if present.

    This implementation:
      - scans cache for ChunkAnalysis manifests
      - loads each payload.pkl
      - merges outputs (accumulate-style) if possible
      - writes merged payload.pkl + merged manifest json
    """
    fileset_path = deps.need(target.fileset)
    fileset_payload = json.loads(fileset_path.read_text())

    dataset = fileset_payload["dataset"]
    era = fileset_payload["era"]
    tag = getattr(target, "tag", None)

    
    cache_root = out.parents[2]
    
    analysis_manifests = _scan_chunk_results(cache_root, dataset, era, tag)

    outputs: List[Any] = []
    used_parts: List[int] = []
    for manifest_path in analysis_manifests:
        manifest = json.loads(manifest_path.read_text())
        payload_name = manifest.get("payload", "payload.pkl")
        payload_path = manifest_path.parent / payload_name
        if not payload_path.exists():
            continue

        with payload_path.open("rb") as f:
            outputs.append(cloudpickle.load(f))

        if isinstance(manifest.get("part"), int):
            used_parts.append(manifest["part"])

    merged_output: Any = None
    if outputs:
        try:
            from coffea.processor.accumulator import accumulate
            merged_output = accumulate(outputs)
        except Exception:
            merged_output = outputs

    merged_payload_path = out.parent / "payload.pkl"
    with merged_payload_path.open("wb") as f:
        cloudpickle.dump(merged_output, f)

    out.write_text(
        json.dumps(
            {
                "type": "MergedResult",
                "dataset": dataset,
                "era": era,
                "tag": tag,
                "n_parts": len(set(used_parts)) if used_parts else 0,
                "parts": sorted(set(used_parts)) if used_parts else [],
                "n_inputs": len(outputs),
                "payload": merged_payload_path.name,
                "parameters": target.keys(),
            },
            indent=2,
        )
    )

@producer(Plots)
def make_plots(*, target: Plots, deps: Deps, out: Path) -> None:
    """
    Placeholder plots artifact that depends on MergedResult.
    """
    merged_path = deps.need(MergedResult(fileset=target.fileset, tag=target.tag))
    merged = json.loads(merged_path.read_text())
    payload = {
        "dataset": merged["dataset"],
        "era": merged["era"],
        "tag": merged["tag"],
        "n_files": merged.get("n_files", 0),
        "plots": [],
        "note": "Placeholder plot manifest.",
    }
    out.write_text(json.dumps(payload, indent=2))
