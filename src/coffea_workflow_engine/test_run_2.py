import coffea_workflow_engine.workflow.model as mdl
import coffea_workflow_engine.workflow.config as cfg
import coffea_workflow_engine.workflow.render as rnd
from coffea_workflow_engine.artifacts import Fileset, Partition

workflow = mdl.Workflow()

step_fileset = workflow.add(
    mdl.Step(
        name="fileset",
        target_type=Fileset,
        params={"dataset": "TTbar", "era": "2018"},
    )
)

step_partition = workflow.add(
    mdl.Step(
        name="partition",
        target_type=Partition,
        params={
                  "fileset_ref": {"type": "Fileset", "key": {"dataset": "TTbar", "era": "2018"}},
                  "n_parts": 3,
                },
    ),
    depends_on=[step_fileset],
)

config = cfg.Config(renderer="local", cache_dir=".cache")
local_root_task = rnd.render(workflow, config)
print("Successfully rendered workflow!")
