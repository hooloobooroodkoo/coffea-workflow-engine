from pathlib import Path
from coffea_workflow_engine.executor import Executor
from coffea_workflow_engine.artifacts import Fileset, Partition
import coffea_workflow_engine.default_producers  # ensure producers registered

ex = Executor(cache_dir=Path(".cache"))
fs = Fileset(dataset="TTbar", era="2018")
p = Partition(fileset=fs, n_parts=3)

out = ex.materialize(p)
print("Partition manifest:", out)
print(out.read_text())