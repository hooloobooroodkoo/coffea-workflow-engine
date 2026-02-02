from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Mapping, Protocol, runtime_checkable
from .identity import hash_identity

ARTIFACT_REGISTRY: dict[str, type[Artifact]] = {}

def register_artifact(cls: type[Artifact]):
    ARTIFACT_REGISTRY[cls.__name__] = cls
    return cls

def artifact_from_dict(d: dict) -> Artifact:
    t = d["type"]
    try:
        cls = ARTIFACT_REGISTRY[t]
    except KeyError as e:
        raise ValueError(f"Unknown artifact type: {t}") from e
    return cls(**d["key"])
    
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
class Fileset(ArtifactBase):
    dataset: str
    era: str

    def keys(self) -> Mapping[str, Any]:
        return {"dataset": self.dataset, "era": self.era}


@register_artifact
@dataclass(frozen=True)
class Partition(ArtifactBase):
    fileset: Fileset
    n_parts: int
    strategy: str = "simple"  # think of strategies and how to reconcile them with n_parts

    def keys(self) -> Mapping[str, Any]:
        return {"fileset": self.fileset.keys(), "n_parts": self.n_parts, "strategy": self.strategy}



@register_artifact
@dataclass(frozen=True)
class ChunkResult(ArtifactBase):
    fileset: Fileset
    part: int
    chunk_size: int
    tag: str # we need to preserve the processor/runner code itself
    

    def keys(self) -> Mapping[str, Any]:
        return {
            "fileset": self.fileset.keys(),
            "part": self.part,
            "chunk_size": self.chunk_size,
            "tag": self.tag,
        }


@register_artifact
@dataclass(frozen=True)
class MergedResult(ArtifactBase):
    fileset: Fileset
    tag: str

    def keys(self) -> Mapping[str, Any]:
        return {"fileset": self.fileset.keys(), "tag": self.tag}


@register_artifact
@dataclass(frozen=True)
class Plots(ArtifactBase):
    fileset: Fileset
    tag: str

    def keys(self) -> Mapping[str, Any]:
        return {"fileset": self.fileset.keys(), "tag": self.tag}
