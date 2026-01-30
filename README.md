# coffea-workflow-engine #
A tiny workflow engine for Coffea-style analyses, built around **typed artifacts** and **producers**.

Instead of describing a workflow DAG as a YAML graph of steps, it **requests a target artifact** (e.g. `Partition(...)`, `Plots(...)`) and the engine resolves & materializes the dependency graph by calling producer functions.


## Concept

### Artifacts
An **Artifact** is a typed description of an output that can be materialized to disk.

For example:
- `Fileset(...)` – resolved list/map of input files
- `Partition(fileset=..., n_parts=...)` – partition manifest
- `ChunkResult(partition=..., chunk_id=...)` – partial Coffea output
- `MergedResult(...)` – merged output from all chunks
- `Plots(...)` – rendered plots

Artifacts are *not* the data themselves. They’re *requests* describing what should exist.

Each artifact provides:
- `keys(): Mapping[str, JsonLike]` – the parameter slice that defines this artifact
- `identity(): str` – stable hash derived from `(type_name + canonical(keys) [+ later upstream/code?/env?])`
- `type_name: str` – class name

### Producers
A **producer** is a python function that knows how to build one artifact type.

```python
@producer(Fileset)
def make_fileset(target: Fileset, deps: Deps, out: Path) -> None:
    ...
    out.write_text(...)
```
