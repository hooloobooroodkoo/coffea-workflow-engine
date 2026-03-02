# src/coffea_workflow_engine/default_producers.py
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Dict, List, Any
import importlib
import inspect
import subprocess

import cloudpickle

from .artifacts import Fileset, Chunking, ChunkAnalysis, MergedResult, Plots, CustomArtifact
from .deps import Deps
from .producers import producer


def _load_fileset_source():
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

def import_callable(dotted: str):
    mod, fn = dotted.split(":")
    m = importlib.import_module(mod)
    return getattr(m, fn)

@producer(CustomArtifact)
def run_custom_artifact(*, producer_name: str, params: Dict[str, Any], out_dir: Path) -> Dict[str, Any]:
    """
    Calls producer_name(**params) and writes payload.json.
    If the producer returns a dict, store it as payload["result"].
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    fn = import_callable(producer_name)
    result = fn(**params)

    payload: Dict[str, Any] = {
        "producer": producer_name,
        "params": params,
        "result": result if isinstance(result, dict) else None,
    }

    (out_dir / "payload.json").write_text(json.dumps(payload, indent=2, sort_keys=True))
    return payload

        

@producer(Fileset)
def make_fileset(*, target: Fileset, deps: Deps, out: Path) -> None:
    
    fn = _load_object(target.builder) # call the function that user created
    fileset_dict = fn(**(target.builder_params or {}))

    if not isinstance(fileset_dict, dict):
        raise TypeError("Fileset builder must return a dict")

    if target.dataset not in fileset_dict:
        raise KeyError(f"Dataset key '{target.dataset}' not in fileset builder result. "
                       f"Available keys: {list(fileset_dict)[:10]}...")

    entry = fileset_dict[target.dataset]
    if not isinstance(entry, dict) or "files" not in entry:
        raise TypeError(f"Fileset entry for '{target.dataset}' must be a dict with 'files'")

    payload = {
        "dataset": target.dataset,
        "era": target.era,
        "files": entry["files"],
        "metadata": entry.get("metadata", {}),
        "builder": target.builder,
        "builder_params": target.builder_params,
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
    Initiate an object.
    For example:
    "builder": "utils.file_input:construct_fileset"
    or
    "processor": "processor:MyProcessor"
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

def _resolve_executor(executor, executor_params):
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

def _call_with_accepted_kwargs(fn, kwargs: Dict[str, Any]):
    sig = inspect.signature(fn)
    params = sig.parameters
    accepts_kwargs = any(p.kind == p.VAR_KEYWORD for p in params.values())
    if accepts_kwargs:
        return fn(**kwargs)
    filtered = {k: v for k, v in kwargs.items() if k in params}
    return fn(**filtered)
    
@producer(ChunkAnalysis)
def make_chunk_analysis(*, target: ChunkAnalysis, deps: Deps, out: Path) -> None:
    """
    Runs a Coffea processor on one partition from a Chunking manifest.
    """
    from coffea.nanoevents import NanoAODSchema
    from coffea.processor.executor import Runner
    from coffea.processor import ProcessorABC

    chunking_path = deps.need(target.chunk)
    chunking_payload = json.loads(chunking_path.read_text())
    
    parts = chunking_payload.get("parts", [])
    if not isinstance(parts, list) or not parts:
        raise ValueError("ChunkAnalysis requires Chunking with non-empty 'parts'")
    
    part_entry = next(
        (p for p in parts if isinstance(p, dict) and p.get("part") == target.part),
        None,
    )
    if part_entry is None:
        raise IndexError(f"ChunkAnalysis part={target.part} not found in Chunking manifest")
    
    files = part_entry.get("files", [])
    if not isinstance(files, list) or not files:
        raise ValueError(f"ChunkAnalysis part={target.part} has empty 'files'")

    fileset = _fileset_from_list_payload(chunking_payload, files)
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
                "type": "ChunkAnalysis",
                "tag": target.tag,
                "part": target.part,
                "payload": payload_path.name,
                "chunk_files": files,
                "parameters": target.keys(),
            },
            indent=2,
        )
    )

@producer(MergedResult)
def make_merged_result(*, target: MergedResult, deps: Deps, out: Path) -> None:
    inputs_info = []
    outputs = []

    for chunk in target.inputs:
        manifest_path = deps.need(chunk)             
        manifest = json.loads(manifest_path.read_text())

        payload_name = manifest.get("payload", "payload.pkl")
        payload_path = manifest_path.parent / payload_name
        if not payload_path.exists():
            raise FileNotFoundError(f"Missing ChunkAnalysis payload: {payload_path}")

        with payload_path.open("rb") as f:
            outputs.append(cloudpickle.load(f))

        inputs_info.append({
            "part": manifest.get("part"),
            "tag": manifest.get("tag"),
            "payload_json": str(manifest_path),
            "payload_pkl": payload_name,
            "n_files": len(manifest.get("chunk_files", [])),
        })

    merged_obj = None
    merge_strategy = None
    if outputs:
        try:
            from coffea.processor.accumulator import accumulate
            merged_obj = accumulate(outputs)
            merge_strategy = "coffea.accumulate"
        except Exception:
            merged_obj = outputs
            merge_strategy = "list"

    merged_payload_path = out.parent / "payload.pkl"
    with merged_payload_path.open("wb") as f:
        cloudpickle.dump(merged_obj, f)

    out.write_text(json.dumps({
        "type": "MergedResult",
        "tag": target.tag,
        "merge_strategy": merge_strategy,
        "n_inputs": len(outputs),
        "inputs": inputs_info,
        "payload": merged_payload_path.name,
        "parameters": target.keys(),
    }, indent=2))

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
