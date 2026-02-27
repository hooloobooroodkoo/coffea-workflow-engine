from coffea_workflow_engine.workflow.workflow import Workflow, Step
from coffea_workflow_engine.workflow.config import Config
import coffea_workflow_engine.workflow.render as rnd
from coffea_workflow_engine.artifacts import Fileset, Chunking
from ...examples.agc_demo_coffea_workflow.utils.file_input import construct_fileset
from ...executor import Executor
from ...artifacts import artifact_from_dict
from pathlib import Path
from .utils import _topo_order, _resolve_params, _print_dag
import luigi
from luigi import LocalTarget
import json
import pickle


workflow = Workflow()

step_fileset = workflow.add(
    Step(
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
    Step(
        name="chunking",
        step_type=Chunking,
        params={
                  "fileset_ref": "fileset",
                  "n_parts": 3,
                },
    ),
    depends_on=[step_fileset],
)

# analysis_params_common = {
#     "chunk_ref": "chunking",
#     "treename": "Events",
#     "chunk_size": 50_000,
#     "processor": "processor:MyProcessor",
#     "processor_params": {},
#     "executor": "futures",
#     "executor_params": {},
#     "tag": "demo",
# }

# analysis_steps = workflow.add_chunk_analyses(
#     name_prefix="analysis_chunk",
#     step_type=ChunkAnalysis,
#     n_parts=3,
#     common_params=analysis_params_common,
#     depends_on=[step_chunking],
# )


# analysis_step_names = [s.name for s in analysis_steps]

# step_merge = workflow.add(
#     mdl.Step(
#         name="merge",
#         step_type=MergedResult,
#         params={
#             "inputs_ref": analysis_step_names,
#             "tag": "ttbar__nominal_chunk_analysis",
#         },
#     ),
#     depends_on=analysis_steps,
# )
cache_dir_name = ".cache_luigi_test"
config = Config(renderer="luigi", cache_dir=cache_dir_name)

def cache_path(*parts):
    p = Path(".cache_luigi").joinpath(*parts)
    p.parent.mkdir(parents=True, exist_ok=True)
    return str(p)

def write_json(target: LocalTarget, obj):
    with target.open("w") as f:
        json.dump(obj, f, indent=2, sort_keys=True)

def read_json(target: LocalTarget):
    with target.open("r") as f:
        return json.load(f)

def write_pickle(target, obj):
    with open(target.path, "wb") as f:
        pickle.dump(obj, f, protocol=pickle.HIGHEST_PROTOCOL)

def read_pickle(target):
    with open(target.path, "rb") as f:
        return pickle.load(f)
    
class ArtifactTask(luigi.Task):
    """
    The ArtifactTask works is: 
    """
    # every Step's (Artifact's) parameters are saved in spec.json where spec is artifact_id
    # I want to preserve the code version and to make it being updated autamatically when the code is changed???
    artifact_id = luigi.Parameter()
    cache_dir = luigi.Parameter()
    spec_dir = luigi.Parameter()
    run_version = luigi.Parameter()
    
    def _spec_path(self):
        return Path(self.spec_dir) / self.artifact_id / "payload.json"
    
    def _read_spec(self):
        return json.loads(self._spec_path().read_text())
    


    # goes through the Step's depends_on field in spec file and recursively initializes ArtifactTask for every previous step
    def requires(self):
        spec = self._read_spec()
        deps = spec.get("deps", []) # or "depends_on"???
        return [
            ArtifactTask(
                artifact_id=dep if isinstance(dep, str) else dep["id"], #???
                spec_dir=self.spec_dir,
                cache_dir=self.cache_dir,
                run_version = self.run_version
            )
            for dep in deps
        ]
    def output(self):
        spec = self._read_spec()
        type_name = spec["type"]

        out_path = Path(self.cache_dir) / type_name / self.artifact_id / "payload.json"
        return LocalTarget(str(out_path))

    # materializes the Artifact
    def run(self):
        spec = self._read_spec()

        artifact = artifact_from_dict({"type": spec["type"], "key": spec["keys"]})
        executor = Executor(cache_dir=Path(self.cache_dir))
        executor.materialize(artifact)
        try:
            check_output_creation = self._read_spec()
        except Exception as e:
            print("Executor created path for the output was not found.")
            print(e)
            
        # if breaks than check whether materialize saved it to the output path

    
def render_luigi(workflow: Workflow, config: Config):
    """
    The way the render works: the coffea-workflow-engine DAG is universal.
    I want to translate it into luigi tasks. I will iterate through the DAG nodes starting from the last step
    and creating a general luigi task ArtifactTask(luigi.Task) from the last step of workflow.  
    The requires() function will go through my Artifact's dependencies (depends_on from workflow.Step). It will
    recursively create ArtifactTasks for all the dependencies. Then starting from the first step run() of the 
    luigi.Task will be called and in the run I'm materilizing the Artifact and writing id down. The output()
    points to the same cache path the executor writes to.
    
    My first limitation is that complicated Artifacts' types can not be easily passed to luigi.Parameter() and
    luigi.SpecificParameter() require easily serializable/deserializable input. That's why the way task is
    excepting the parameters is done through a spec files. While going through all the tasks the parameters
    writing down to json files is done. Each file has a unique hash name and contains a json
    with its parameters. 
    """
    cache_dir = Path(config.cache_dir)
    spec_dir = cache_dir / "specs" # should I add code version here???
    spec_dir.mkdir(parents=True, exist_ok=True)

    # write specs (same resolution as local_render, but no materialize)
    order = _topo_order(len(workflow.steps), workflow.edges)
    artifacts_by_name = {}
    id_by_name = {}

    num_steps = len(workflow.steps)
    if num_steps == 0:
        return "No steps found in the workflow."

    _print_dag(workflow)
    def build_incoming(num_steps, edges):
        incoming = {i: [] for i in range(num_steps)}
        for dep, step in edges:
            incoming[step].append(dep)
        return incoming
    incoming = build_incoming(num_steps, workflow.edges)

    artifacts_by_name = {}   # for _resolve_params placeholders
    id_by_idx = {}           # step index -> artifact_id

    for idx in order:
        step = workflow.steps[idx]
        params = _resolve_params(step.params, artifacts_by_name)
        artifact = step.step_type(**params)

        aid = artifact.identity()
        id_by_idx[idx] = aid
        artifacts_by_name[step.name] = artifact

        dep_ids = [id_by_idx[dep_idx] for dep_idx in incoming[idx]]

        spec = {
            "id": aid,
            "type": artifact.type_name,
            "keys": artifact.keys(),
            "deps": dep_ids,
        }
        (spec_dir / f"{aid}.json").write_text(json.dumps(spec, indent=2, sort_keys=True))

    target_step = workflow.steps[order[-1]].name
    target_id = id_by_name[target_step]

    return {
        "spec_dir": str(spec_dir),
        "cache_dir": str(cache_dir),
        "target_id": target_id,
        "target_step": target_step,
        
        # user:
        "luigi dag is ready!\nRun the following command:\n": (
            "python -m luigi --module coffea_workflow_engine.workflow.renderers.render_luigi"
            "ArtifactTask --artifact-id {target_id} --spec-dir {spec_dir} "
            "--cache-dir {cache_dir} --local-scheduler"
        ).format(target_id=target_id, spec_dir=spec_dir, cache_dir=cache_dir),
    }

    
render_luigi(workflow, config)



##############################################
# added to show the workflow in more details
##############################################
# print("Successfully rendered workflow!\n")

# import json, glob
# import pprint
# import cloudpickle
# from pathlib import Path
# print("Intermediate results:\n")
# print(f"\nSTEP Fileset: {cache_dir_name}/Fileset/*/payload.json")
# for p in glob.glob(f"{cache_dir_name}/Fileset/*/payload.json"):
#     print(p)
#     d = json.load(open(p))
#     pprint.pprint(d)
    
# print(f"\nSTEP Chunking: {cache_dir_name}/Chunking/*/payload.json")
# for p in glob.glob(f"{cache_dir_name}/Chunking/*/payload.json"):
#     d = json.load(open(p))
#     print("==", p)
#     for part in d.get("parts", []):
#         print("part", part["part"], "n_files", len(part["files"]))

# import json, glob
# import cloudpickle
# from pathlib import Path

# print(f"\nSTEP ChunkAnalysis: {cache_dir_name}/ChunkAnalysis/*/payload.json")
# for p in glob.glob(f"{cache_dir_name}/ChunkAnalysis/*/payload.json"):
#     print(p)
#     d = json.load(open(p))

#     print(d.get("chunk_files", []))

#     # what processor returned per chunk
#     chunk_dir = Path(p).parent
#     chunk_pkl = chunk_dir / d.get("payload", "payload.pkl")

#     with chunk_pkl.open("rb") as f:
#         chunk_obj = cloudpickle.load(f)
#         chunk_nevents = chunk_obj["nevents"]
#     print(f"part={d.get('part')}, tag={d.get('tag')}, nevents={chunk_nevents}\n")


# print(f"\nSTEP MergedResults: {cache_dir_name}/MergedResult/*/payload.json")
# for p in glob.glob(f"{cache_dir_name}/MergedResult/*/payload.json"):
#     print(p)
#     d = json.load(open(p))

#     parts = sorted({x.get("part") for x in d.get("inputs", []) if isinstance(x.get("part"), int)})

#     merged_dir = Path(p).parent
#     merged_pkl = merged_dir / d.get("payload", "payload.pkl")

#     with merged_pkl.open("rb") as f:
#         merged_obj = cloudpickle.load(f)
#         merged_nevents = merged_obj["nevents"]

#     print(
#         f"merged parts: {parts}, strategy={d.get('merge_strategy')}, "
#         f"n_inputs={d.get('n_inputs')}, merged_nevents={merged_nevents}"
#     )


    
    



