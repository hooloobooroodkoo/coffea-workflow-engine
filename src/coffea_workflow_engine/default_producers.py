# src/coffea_workflow_engine/default_producers.py
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Dict, List, Any

from .artifacts import Fileset, Partition, ChunkResult, MergedResult, Plots
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


@producer(Partition)
def make_partition(*, target: Partition, deps, out: Path) -> None:
    """
    Partition a Fileset manifest into N parts and write partition manifest.
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

@producer(ChunkResult)
def make_chunk_result(*, target: ChunkResult, deps: Deps, out: Path) -> None:
    """
    Create a chunk manifest based on a Fileset and chunk_size.
    """
    fileset_path = deps.need(target.fileset)
    fileset = json.loads(fileset_path.read_text())

    files = fileset["files"]
    chunk_size = target.chunk_size
    if chunk_size <= 0:
        raise ValueError("chunk_size must be > 0")

    chunks = [files[i:i + chunk_size] for i in range(0, len(files), chunk_size)]
    if target.part < 0 or target.part >= len(chunks):
        raise IndexError(f"Chunk part {target.part} out of range for {len(chunks)} chunks")

    payload = {
        "dataset": fileset["dataset"],
        "era": fileset["era"],
        "tag": target.tag,
        "part": target.part,
        "chunk_size": chunk_size,
        "files": chunks[target.part],
    }
    out.write_text(json.dumps(payload, indent=2))


def _scan_chunk_results(cache_root: Path, dataset: str, era: str, tag: str) -> List[Dict[str, Any]]:
    chunk_dir = cache_root / "ChunkResult"
    if not chunk_dir.exists():
        return []

    results: List[Dict[str, Any]] = []
    for payload_path in chunk_dir.rglob("payload.json"):
        try:
            payload = json.loads(payload_path.read_text())
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
    Merge available ChunkResult manifests for the same dataset/era/tag, if present.
    """
    cache_root = out.parents[2]
    chunks = _scan_chunk_results(cache_root, target.fileset.dataset, target.fileset.era, target.tag)

    merged_files: List[str] = []
    for chunk in chunks:
        merged_files.extend(chunk.get("files", []))

    payload = {
        "dataset": target.fileset.dataset,
        "era": target.fileset.era,
        "tag": target.tag,
        "n_chunks": len(chunks),
        "n_files": len(merged_files),
        "files": merged_files,
    }
    out.write_text(json.dumps(payload, indent=2))


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
