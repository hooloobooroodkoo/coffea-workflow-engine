from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Mapping, Protocol, runtime_checkable
from .identity import hash_identity

@runtime_checkable
class Artifact(Protocol):
    def keys(self) -> Mapping[str, Any]: ...
    def identity(self) -> str: ...
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
        return hash_identity(self.type_name, self.keys())

@dataclass(frozen=True)
class Fileset(ArtifactBase):
    dataset: str
    era: str

    def keys(self) -> Mapping[str, Any]:
        return {"dataset": self.dataset, "era": self.era}


@dataclass(frozen=True)
class Partition(ArtifactBase):
    fileset: Fileset
    n_parts: int

    def keys(self) -> Mapping[str, Any]:
        return {"fileset": self.fileset.keys(), "n_parts": self.n_parts}


@dataclass(frozen=True)
class ChunkResult(ArtifactBase):
    fileset: Fileset
    part: int
    chunk_size: int

    def keys(self) -> Mapping[str, Any]:
        return {
            "fileset": self.fileset.keys(),
            "part": self.part,
            "chunk_size": self.chunk_size,
        }


@dataclass(frozen=True)
class MergedResult(ArtifactBase):
    fileset: Fileset
    tag: str

    def keys(self) -> Mapping[str, Any]:
        return {"fileset": self.fileset.keys(), "tag": self.tag}


@dataclass(frozen=True)
class Plots(ArtifactBase):
    fileset: Fileset
    tag: str

    def keys(self) -> Mapping[str, Any]:
        return {"fileset": self.fileset.keys(), "tag": self.tag}
