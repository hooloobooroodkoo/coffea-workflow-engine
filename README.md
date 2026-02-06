# coffea-workflow-engine #
A tiny workflow engine for Coffea-style analyses, built around **typed artifacts** and **producers**.

Instead of describing a workflow DAG as a YAML graph of steps, it **requests a target artifact** (e.g. `Partition(...)`, `Plots(...)`) and the engine resolves & materializes the dependency graph by calling producer functions.


## Concept

### Workflow
A DAG builder user can use it for structuring the analysis.
A workflow is a list of Steps plus dependency edges:
- A Step names what to build (step_type) and with which parameters (params)
- Dependencies define the execution order (and allow referencing earlier step outputs)
- *Idea*: to keep the engine core backend-neutral, and make adding Luigi/Snakemake/Law/CWL later much easier via implementing different renders (right now only local_render is available). More in issue #1.
```python
from coffea_workflow_engine.workflow.workflow import Workflow, Step
from coffea_workflow_engine.workflow.config import Config
from coffea_workflow_engine.workflow.render import render

# 1) Build the workflow (steps + dependencies)
workflow = Workflow()
# ... add Steps ...
step_fileset = workflow.add(
    Step(
        name="fileset",
        step_type=Fileset,
        params={...})
    )

# 2) Choose how to run it
config = Config(
    renderer="local",   # currently supported: "local", next to add "luigi"
    cache_dir=".cache", # where artifact payloads are stored
)

# 3) Render (execute) the workflow
result = render(workflow, config)

# result contains step -> paths/artifacts and execution order (renderer-dependent)
```

### Artifacts
An **Artifact** is a typed description of an output that can be materialized to disk.

For example:
- `Fileset(...)` – resolved list/map of input files
- `Chunking(fileset=..., n_parts=...)` – partition manifest
- `ChunkAnalysis(chunking=..., chunk_id=...)` – partial Coffea output
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
`target` is the artifact instance (has keys like dataset/era/n_parts/etc.)\
`deps.need(other_artifact)` triggers building dependencies and returns the dependency’s path on disk\
`out` is the output path assigned by the executor for target

### Executor

The Executor:

1. Computes the artifact’s identity()
2. Maps identity → a cache path on disk
3. If cached: returns the path
4. Otherwise: calls the registered producer, writing the output
5. Returns the path

As a result, we have:
- implicit DAG from Python
- automatic caching
- the foundation for resumability and reproducibility

### Run example

```bash
git clone https://github.com/hooloobooroodkoo/coffea-workflow-engine.git
cd coffea-workflow-engine

# develop / editable install
SETUPTOOLS_SCM_PRETEND_VERSION=0.0.0 python -m pip install -e .

# run the demo
cd src/coffea_workflow_engine/example
python test_workflow_run.py
```
![](docs/workflow_example_1.png)
![](docs/workflow_example_2.png)
