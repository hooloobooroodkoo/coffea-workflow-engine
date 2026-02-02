# src/coffea_workflow_engine/default_producers.py
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Dict, List

from .artifacts import Fileset, Partition, MergedResult
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
    p = Path(os.environ.get("COFFEA_FILESET_JSON", "filesets.json"))
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

@producer(MergedResult)
def make_merged(target: MergedResult, deps: Deps, out: Path) -> None:
    
    chunk_paths = []
    for chunk_id in range(10):
        cr = ChunkResult(dataset=target.dataset, era=target.era, chunk_id=chunk_id)
        chunk_paths.append(deps.need(cr))

    
    out.write_bytes(b"...")