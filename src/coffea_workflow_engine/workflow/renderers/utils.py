from ...artifacts import artifact_from_dict
from ..workflow import Workflow

def _topo_order(num_steps, edges):
    """
    This function sorts the steps in the workflow dag 
    and builds the queue of their step-by-step execution.
    """
    outgoing = {i: [] for i in range(num_steps)}
    in_deg = {i: 0 for i in range(num_steps)}
    for src, dst in edges:
        outgoing[src].append(dst)
        in_deg[dst] += 1

    queue = [i for i in range(num_steps) if in_deg[i] == 0]
    order= []
    while queue:
        idx = queue.pop(0)          
        order.append(idx)
        for nxt in outgoing[idx]:
            in_deg[nxt] -= 1
            if in_deg[nxt] == 0:
                queue.append(nxt)

    if len(order) != num_steps:
        raise ValueError("Workflow has a cycle or disconnected dependency graph")
    return order

def _resolve_params(raw_params, artifacts_by_name):
    """
    
    """
    def resolve_value(key, value):
        if isinstance(value, dict) and "type" in value and ("key" in value or "keys" in value):
            return artifact_from_dict(value)

        if key.endswith("_ref") and isinstance(value, str) and value in artifacts_by_name:
            return artifacts_by_name[value]

        if isinstance(value, (list, tuple)):
            resolved_items = [resolve_value(key, v) for v in value]
            return type(value)(resolved_items)

        return value

    resolved = {}
    for key, value in raw_params.items():
        target_key = key[:-4] if key.endswith("_ref") else key
        resolved[target_key] = resolve_value(key, value)

    return resolved

def _print_dag(workflow: Workflow) -> None:
    print("Workflow DAG:")
    if not workflow.steps:
        print("  (no steps)")
        return
    for idx, step in enumerate(workflow.steps):
        print(
            f"  [{idx}] {step.name} -> {step.step_type.__name__} params={step.params}"
        )
    if workflow.edges:
        print("Edges:")
        for src, dst in workflow.edges:
            print(f"  {workflow.steps[src].name} -> {workflow.steps[dst].name}")
    else:
        print("Edges: (none)")