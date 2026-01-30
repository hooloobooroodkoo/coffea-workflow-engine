# add upstream_identities , something else?
from __future__ import annotations
import hashlib
import json
from typing import Any

def canonicalize(obj: Any) -> bytes:
    """
    Convert obj -> stable JSON bytes.
    - sort keys
    - no whitespace
    - only JSONable content
    """
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")

def hash_identity(*parts: Any) -> str:
    h = hashlib.sha256()
    for p in parts:
        if isinstance(p, (bytes, bytearray)):
            h.update(p)
        else:
            h.update(canonicalize(p))
        h.update(b"|")
    return h.hexdigest()
