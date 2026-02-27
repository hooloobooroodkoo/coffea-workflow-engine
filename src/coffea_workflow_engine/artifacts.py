from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Mapping, Protocol, Dict, List, runtime_checkable
from .identity import hash_identity


ARTIFACT_REGISTRY: dict[str, type[Artifact]] = {}

def register_artifact(cls: type[Artifact]):
    ARTIFACT_REGISTRY[cls.__name__] = cls
    return cls

def artifact_from_dict(d: dict) -> Artifact:
    """
    This function searches for the certain artifact type
    in the registry and creates and returns the Artifact object if found.
    """
    t = d["type"]
    try:
        cls = ARTIFACT_REGISTRY[t]
    except KeyError as e:
        raise ValueError(f"Unknown artifact type: {t}") from e
    key = d.get("key", None)
    if key is None:
        key = d.get("keys", None)
    if key is None:
        raise ValueError("Artifact dict must include 'key' or 'keys'")
    
    
    def _resolve(value: Any) -> Any:
        if isinstance(value, dict) and "type" in value and ("key" in value or "keys" in value):
            return artifact_from_dict(value)
        if isinstance(value, list):
            return [_resolve(item) for item in value]
        if isinstance(value, tuple):
            return tuple(_resolve(item) for item in value)
        if isinstance(value, dict):
            return {inner_k: _resolve(inner_v) for inner_k, inner_v in value.items()}
        return value
    
    resolved = {}
    for k, v in key.items():
        resolved[k] = _resolve(v)
    return cls(**resolved)
    
@runtime_checkable
class Artifact(Protocol):
    def keys(self) -> Mapping[str, Any]: ...
    def identity(self) -> str: ...
    def to_dict(self) -> dict: ...
    @property
    def type_name(self) -> str: ...

@dataclass(frozen=True)
class ArtifactBase:
    def keys(self) -> Mapping[str, Any]:
        raise NotImplementedError

    @property
    def type_name(self) -> str:
        return type(self).__name__

    def identity(self) -> str:
        return hash_identity(self.to_dict())

    def to_dict(self) -> dict:
        return {"type": self.__class__.__name__, "keys": self.keys()}

@register_artifact
@dataclass(frozen=True)
class CustomArtifact(ArtifactBase):
    """
    Generic artifact that delegates production to a user-provided callable/script name.
    """

    name: str
    producer_name: str
    params: Dict[str, Any] = field(default_factory=dict)
    dependencies: List[ArtifactBase] = field(default_factory=list)

    def keys(self) -> Mapping[str, Any]:
        return {
            "name": self.name,
            "producer_name": self.producer_name,
            "params": self.params,
            "dependencies": [dep.keys() for dep in self.dependencies],
        }

@register_artifact
@dataclass(frozen=True)
class Fileset(ArtifactBase):
    dataset: str
    era: str
    builder: str = "" # uses function that user provides
    builder_params = field(default_factory=dict)

    def keys(self) -> Mapping[str, Any]:
        return {
            "dataset": self.dataset,
            "era": self.era,
            "builder": self.builder,
            "builder_params": self.builder_params,
        }


@register_artifact
@dataclass(frozen=True)
class Chunking(ArtifactBase):
    fileset: Fileset
    n_parts: int
    strategy: str = "simple"  # think of strategies and how to reconcile them with n_parts

    def keys(self) -> Mapping[str, Any]:
        return {"fileset": self.fileset.keys(), "n_parts": self.n_parts, "strategy": self.strategy}

@register_artifact
@dataclass(frozen=True)
class ChunkAnalysis(ArtifactBase):
    chunk: "Chunking"
    part: int
    chunk_size: int
    tag: str
    processor: str
    processor_params = field(default_factory=dict)
    treename: str = "Events"
    executor: str = "futures"
    executor_params = field(default_factory=dict)

    def keys(self) -> Mapping[str, Any]:
        return {
            "chunk": self.chunk.keys(),
            "part": self.part,
            "chunk_size": self.chunk_size,
            "tag": self.tag,
            "processor": self.processor,
            "processor_params": self.processor_params,
            "treename": self.treename,
            "executor": self.executor,
            "executor_params": self.executor_params,
        }


@register_artifact
@dataclass(frozen=True)
class MergedResult(ArtifactBase):
    inputs: List[ChunkAnalysis, ...]
    tag: str

    def keys(self) -> Mapping[str, Any]:
        inputs: list[Any] = []
        for i in self.inputs:
            if hasattr(i, "keys"):
                inputs.append(i.keys())
            elif isinstance(i, dict):
                inputs.append(i)
            else:
                inputs.append(i)
        return {"inputs": inputs, "tag": self.tag}


@register_artifact
@dataclass(frozen=True)
class Plots(ArtifactBase):
    source: ArtifactBase
    plotter: str
    plotter_params: Dict[str, Any] = field(default_factory=dict)

    def keys(self) -> Mapping[str, Any]:
        return {
            "source": self.source.keys(),
            "plotter": self.plotter,
            "plotter_params": self.plotter_params,
        }


@register_artifact
@dataclass(frozen=True)
class Plots(ArtifactBase):
    fileset: Fileset
    tag: str

    def keys(self) -> Mapping[str, Any]:
        return {"fileset": self.fileset.keys(), "tag": self.tag}
