import json
from pathlib import Path
import luigi
from luigi import LocalTarget
import importlib
import pickle

# ---- helpers ----

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
        
def make_partition(fileset, n_parts):

    if n_parts <= 0:
        raise ValueError("n_parts must be > 0")
    if not fileset:
        raise ValueError("Fileset is empty; nothing to partition")

    total_files = 0
    for sample, spec in fileset.items():
        if not isinstance(spec, dict):
            raise TypeError(f"fileset['{sample}'] must be a dict, got {type(spec)}")
        if "files" not in spec:
            raise KeyError(f"fileset['{sample}'] missing required key 'files'")
        if not isinstance(spec["files"], list):
            raise TypeError(f"fileset['{sample}']['files'] must be a list")
        total_files += len(spec["files"])

    if total_files == 0:
        raise ValueError("Fileset has 0 files across all samples; nothing to partition")

    parts = []
    for part_id in range(n_parts):
        parts.append({"part": part_id, "fileset": {}})

    for sample, spec in fileset.items():
        files = spec["files"]
        meta = spec.get("metadata", None)

        buckets = [[] for _ in range(n_parts)]
        for i, f in enumerate(files):
            buckets[i % n_parts].append(f)


        for part_id, bucket_files in enumerate(buckets):
            if not bucket_files:
                continue
            entry = {"files": bucket_files}
            if meta is not None:
                entry["metadata"] = meta
            parts[part_id]["fileset"][sample] = entry

    parts = [p for p in parts if p["fileset"]]

    manifest = {
        "n_parts": n_parts,
        "parts": parts,
        "n_samples": len(fileset),
        "n_files_total": total_files,
    }
    return manifest

def run_analysis(
    chunk_fileset,
    treename,
    chunk_size,
    processor_path,
    processor_params,
    executor,
    executor_params,
    tag,
    part_id,
):
    """
    This is where you plug Coffea (or anything).
    For now, returns a small summary that is pickleable.
    """
    from coffea.nanoevents import NanoAODSchema
    from coffea.processor.executor import Runner

    mod_path, name = processor_path.split(":")
    Proc = getattr(importlib.import_module(mod_path), name)
    processor_instance = Proc(**processor_params)
    if executor not in (None, "futures"):
        raise ValueError(
            f"Only executor='futures' is supported (got {executor!r})."
        )

    from coffea.processor.executor import FuturesExecutor

    workers = int(executor_params.get("workers", 1))
    try:
        executor = FuturesExecutor(workers=workers)
    except TypeError:
        executor = FuturesExecutor(max_workers=workers)
        
    runner = Runner(
        executor=executor,
        schema=NanoAODSchema,
        chunksize=chunk_size,
        savemetrics=True,
    )

    output = runner(
        fileset=chunk_fileset,  
        treename=treename,
        processor_instance=processor_instance,
    )

    if isinstance(output, tuple):
        output = output[0]

    print("[Runner output]:", output)
    if isinstance(output, dict) and "nevents" in output:
        ne = output["nevents"]
        ne_val = int(ne.value) if hasattr(ne, "value") else int(ne)
        print("nevents =", ne_val)

    return output
    
    
def merge_results(results, tag):
    """
    Merge chunk outputs. If they are coffea accumulators,
    you'd do `coffea.processor.accumulate(results)`.
    """
    return {"tag": tag, "n_parts": len(results), "parts": results}

# ---- tasks ----

class FilesetTask(luigi.Task):
    dataset = luigi.Parameter()
    era = luigi.Parameter()
    builder = luigi.Parameter()
    builder_params = luigi.DictParameter(default={})

    def output(self):
        return LocalTarget(cache_path("Fileset", f"{self.dataset}__{self.era}.json"))

    def run(self):
        mod_path, fn_name = self.builder.split(":")
        mod = __import__(mod_path, fromlist=[fn_name])
        fn = getattr(mod, fn_name)
        fileset = fn(**dict(self.builder_params))
        write_json(self.output(), fileset)


class ChunkingTask(luigi.Task):
    dataset = luigi.Parameter()
    era = luigi.Parameter()
    builder = luigi.Parameter()
    builder_params = luigi.DictParameter(default={})
    n_parts = luigi.IntParameter()

    def requires(self):
        return FilesetTask(
            dataset=self.dataset,
            era=self.era,
            builder=self.builder,
            builder_params=self.builder_params,
        )

    def output(self):
        return LocalTarget(cache_path("Chunking", f"{self.dataset}__{self.era}__n{self.n_parts}.json"))

    def run(self):
        fileset = read_json(self.input())
        manifest = make_partition(fileset=fileset, n_parts=self.n_parts)
        # manifest could be [{"part_id": 0, ...}, ...]
        write_json(self.output(), manifest)


class ChunkAnalysisTask(luigi.Task):
    dataset = luigi.Parameter()
    era = luigi.Parameter()
    builder = luigi.Parameter()
    builder_params = luigi.DictParameter(default={})
    n_parts = luigi.IntParameter()
    part_id = luigi.IntParameter()
    treename = luigi.Parameter(default="Events")
    chunk_size = luigi.IntParameter(default=50_000)
    processor = luigi.Parameter()
    processor_params = luigi.DictParameter(default={})
    executor = luigi.Parameter(default="futures")
    executor_params = luigi.DictParameter(default={})
    tag = luigi.Parameter(default="demo")

    def requires(self):
        return ChunkingTask(
            dataset=self.dataset,
            era=self.era,
            builder=self.builder,
            builder_params=self.builder_params,
            n_parts=self.n_parts,
        )

    def output(self):
        return LocalTarget(
            cache_path(
                "chunk",
                f"{self.dataset}__{self.era}__n{self.n_parts}__part{self.part_id}__{self.tag}.pkl",
            )
        )

    def run(self):
        manifest = read_json(self.input())
        parts = manifest["parts"]

        if self.part_id >= len(parts):
            raise ValueError(f"part_id={self.part_id} out of range; parts={len(parts)}")

        chunk_fileset = parts[self.part_id]["fileset"]

        result = run_analysis(
            chunk_fileset=chunk_fileset,
            treename=self.treename,
            chunk_size=self.chunk_size,
            processor_path=self.processor,
            processor_params=dict(self.processor_params),
            executor=self.executor,
            executor_params=dict(self.executor_params),
            tag=self.tag,
            part_id=self.part_id,
        )
        write_pickle(self.output(), result)
        print(f"[ChunkAnalysis] wrote: {self.output().path}")
        print("[ChunkAnalysis] result:", result)


class MergeTask(luigi.Task):
    dataset = luigi.Parameter()
    era = luigi.Parameter()
    builder = luigi.Parameter()
    builder_params = luigi.DictParameter(default={})
    n_parts = luigi.IntParameter()
    treename = luigi.Parameter(default="Events")
    chunk_size = luigi.IntParameter(default=50_000)
    processor = luigi.Parameter()
    processor_params = luigi.DictParameter(default={})
    executor = luigi.Parameter(default="futures")
    executor_params = luigi.DictParameter(default={})

    analysis_tag = luigi.Parameter(default="demo")
    merge_tag = luigi.Parameter(default="ttbar__nominal_chunk_analysis")

    def requires(self):
        return [
            ChunkAnalysisTask(
                dataset=self.dataset,
                era=self.era,
                builder=self.builder,
                builder_params=self.builder_params,
                n_parts=self.n_parts,
                part_id=i,
                treename=self.treename,
                chunk_size=self.chunk_size,
                processor=self.processor,
                processor_params=self.processor_params,
                executor=self.executor,
                executor_params=self.executor_params,
                tag=self.analysis_tag,
            )
            for i in range(self.n_parts)
        ]

    def output(self):
        return LocalTarget(cache_path("merge", f"{self.dataset}__{self.era}__n{self.n_parts}__{self.merge_tag}.pkl"))

    def run(self):
        results = [read_pickle(t) for t in self.input()]
        merged = merge_results(results, tag=self.merge_tag)
        write_pickle(self.output(), merged)


class Workflow(luigi.WrapperTask):
    def requires(self):
        return MergeTask(
            dataset="ttbar__nominal",
            era="2015",
            builder="utils.file_input:construct_fileset",
            builder_params={
                "n_files_max_per_sample": 5,
                "use_xcache": False,
                "af_name": "",
                "local_data_cache": None,
                "input_from_eos": False,
                "xcache_atlas_prefix": None,
            },
            n_parts=3,
            processor="processor:MyProcessor",
            processor_params={},
            executor="futures",
            executor_params={},
            treename="Events",
            chunk_size=50_000,
            analysis_tag="demo",
            merge_tag="ttbar__nominal_chunk_analysis",
        )


if __name__ == "__main__":
    luigi.run()
    
# python luigi_workflow.py Workflow --local-scheduler