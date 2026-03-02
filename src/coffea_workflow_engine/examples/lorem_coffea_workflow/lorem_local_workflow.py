"""
If I want to use the same simple scripts from lorem ananlysis I'm using in snakemake and luigi examples
my workflow still lacks for now 1) understanding what the previous step has produced.
Example:
    Step split produced files with cjunks of text, but next step doesn't know that output rn.
What I want:
    Users should be able to say: “call this Python function with these arguments”, where arguments can reference dependencies and outputs.
"""

"""
Idea, pre-implemented BatchesProcessing step:

def make_batches(input, mode, batch_size=optional):
    read_input()
    
    if mode == "per_chunk":
        batches = [[cid] for cid in ids]
        
    elif mode == "all":
        batches = [ids]
        
    elif mode == "grouped":
        batches = [ids[i:i+batch_size] for i in range(0, len(ids), batch_size)]
    else:
        raise ValueError(mode)

    return path_to_batches
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import coffea_workflow_engine.workflow.workflow as mdl
import coffea_workflow_engine.workflow.config as cfg
import coffea_workflow_engine.workflow.render as rnd

from coffea_workflow_engine.artifacts import CustomArtifact

# from scripts.split import split_file
# from scripts.count_letters import count_letters_file
# from scripts.merge_counts import merge_counts

def build_workflow() -> mdl.Workflow:
    workflow = mdl.Workflow()
    
    split_step = workflow.add(
        mdl.Step(
            name="split",
            step_type=CustomArtifact,
            params={
                "producer": "scripts.split:split_file",
                "args": {
                    "src": "lorem.txt",
                    "chunk_bytes": 200,
                    "out_dir": "chunks_inter_files",
                    "chunk_files": "chunk_{i:03d}.txt",
                },
                "outputs": {"out_dir": "chunks_inter_files"},
            },
        )
    )

    count_step = workflow.add(
        mdl.Step(
            name="count",
            step_type=CustomArtifact,
            params={
                "producer": "scripts.count_letters:count_letters_file",
                
                # use some parameters that will define the batch processing approach: each seperate chunk? batches? one batch?
                "foreach": {"dep": "split", "path": "chunks_inter_files.chunk_files", "as": "chunk_file"}, #let's assume each is processed seperately

                "args": {
                    #???? are they already in for each or should something else be here
                },

                # per-item output name (in that item’s artifact directory, or in a shared dir)
                "outputs": {"counts_file": "chunk_counts_{cid}.json"},
            },
        ),
        depends_on=[split_step],
    )


    merge_step = workflow.add(
        mdl.Step(
            name="merge",
            step_type=CustomArtifact,
            params={
            },
        ),
        depends_on=[count_step],
    )

    return workflow


def main() -> None:
    workflow = build_workflow()
    config = cfg.Config(renderer="local", cache_dir=".cache")
    rnd.render(workflow, config)
    print("Successfully rendered lorem local workflow!")


if __name__ == "__main__":
    main()
