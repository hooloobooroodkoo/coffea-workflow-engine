import coffea_workflow_engine.workflow.workflow as mdl
import coffea_workflow_engine.workflow.config as cfg
import coffea_workflow_engine.workflow.render as rnd
from coffea_workflow_engine.artifacts import Fileset, Chunking

workflow = mdl.Workflow()

step_fileset = workflow.add(
    mdl.Step(
        name="fileset",
        step_type=Fileset,
        params={"dataset": "TTbar", "era": "2018"},
    )
)

step_partition = workflow.add(
    mdl.Step(
        name="partition",
        step_type=Chunking,
        params={
                  "fileset_ref": {"type": "Fileset", "key": {"dataset": "TTbar", "era": "2018"}},
                  "n_parts": 3,
                },
    ),
    depends_on=[step_fileset],
)

config = cfg.Config(renderer="local", cache_dir=".cache")
local_root_task = rnd.render(workflow, config)
print("Successfully rendered workflow!\n")

import json, glob
for p in glob.glob(".cache/Chunking/*/payload.json"):
    d = json.load(open(p))
    if d.get("dataset")=="TTbar" and d.get("era")=="2018":
        print("==", p)
        for part in d.get("parts", []):
            print("part", part["part"], "n_files", len(part["files"]))
