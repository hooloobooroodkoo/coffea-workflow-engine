from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Dict, Any

@dataclass(frozen=True)
class Config:
    renderer: Literal["local", "luigi", "law", "airflow"] = "local"
    cache_dir: Path = Path(".cache")
    engine_opts: Dict[str, Any] = None