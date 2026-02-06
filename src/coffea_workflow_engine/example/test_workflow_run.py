import coffea_workflow_engine.workflow.workflow as mdl
import coffea_workflow_engine.workflow.config as cfg
import coffea_workflow_engine.workflow.render as rnd
from coffea_workflow_engine.artifacts import Fileset, Chunking, ChunkAnalysis, MergedResult
from utils.file_input import construct_fileset


workflow = mdl.Workflow()

step_fileset = workflow.add(
    mdl.Step(
        name="fileset",
        step_type=Fileset,
        params={
            "dataset": "ttbar__nominal",
            "era": "2015",
            "builder": "utils.file_input:construct_fileset",
            "builder_params": {
                "n_files_max_per_sample": 5,
                "use_xcache": False,
                "af_name": "",
                "local_data_cache": None,
                "input_from_eos": False,
                "xcache_atlas_prefix": None,
            },
        },
    )
)

step_chunking = workflow.add(
    mdl.Step(
        name="chunking",
        step_type=Chunking,
        params={
                  "fileset_ref": "fileset",
                  "n_parts": 3,
                },
    ),
    depends_on=[step_fileset],
)

analysis_params_common = {
    "chunk_ref": "chunking",
    "treename": "Events",
    "chunk_size": 50_000,
    "processor": "processor:MyProcessor",
    "processor_params": {},
    "executor": "futures",
    "executor_params": {},
    "tag": "demo",
}

analysis_steps = workflow.add_chunk_analyses(
    name_prefix="analysis_chunk",
    step_type=ChunkAnalysis,
    n_parts=3,
    common_params=analysis_params_common,
    depends_on=[step_chunking],
)


analysis_step_names = [s.name for s in analysis_steps]

step_merge = workflow.add(
    mdl.Step(
        name="merge",
        step_type=MergedResult,
        params={
            "inputs_ref": analysis_step_names,
            "tag": "ttbar__nominal_chunk_analysis",
        },
    ),
    depends_on=analysis_steps,
)

config = cfg.Config(renderer="local", cache_dir=".cache")
result = rnd.render(workflow, config)


##############################################
# added to show the workflow in more details
##############################################
print("Successfully rendered workflow!\n")

import json, glob
import pprint
import cloudpickle
from pathlib import Path
print("Intermediate results:\n")
print("\nSTEP Fileset: .cache/Fileset/*/payload.json")
for p in glob.glob(".cache/Fileset/*/payload.json"):
    print(p)
    d = json.load(open(p))
    pprint.pprint(d)
    
print("\nSTEP Chunking: .cache/Chunking/*/payload.json")
for p in glob.glob(".cache/Chunking/*/payload.json"):
    d = json.load(open(p))
    print("==", p)
    for part in d.get("parts", []):
        print("part", part["part"], "n_files", len(part["files"]))

import json, glob
import cloudpickle
from pathlib import Path

print("\nSTEP ChunkAnalysis: .cache/ChunkAnalysis/*/payload.json")
for p in glob.glob(".cache/ChunkAnalysis/*/payload.json"):
    print(p)
    d = json.load(open(p))

    print(d.get("chunk_files", []))

    # what processor returned per chunk
    chunk_dir = Path(p).parent
    chunk_pkl = chunk_dir / d.get("payload", "payload.pkl")

    with chunk_pkl.open("rb") as f:
        chunk_obj = cloudpickle.load(f)
        chunk_nevents = chunk_obj["nevents"]
    print(f"part={d.get('part')}, tag={d.get('tag')}, nevents={chunk_nevents}\n")


print("\nSTEP MergedResults: .cache/MergedResult/*/payload.json")
for p in glob.glob(".cache/MergedResult/*/payload.json"):
    print(p)
    d = json.load(open(p))

    parts = sorted({x.get("part") for x in d.get("inputs", []) if isinstance(x.get("part"), int)})

    merged_dir = Path(p).parent
    merged_pkl = merged_dir / d.get("payload", "payload.pkl")

    with merged_pkl.open("rb") as f:
        merged_obj = cloudpickle.load(f)
        merged_nevents = merged_obj["nevents"]

    print(
        f"merged parts: {parts}, strategy={d.get('merge_strategy')}, "
        f"n_inputs={d.get('n_inputs')}, merged_nevents={merged_nevents}"
    )


    
    



