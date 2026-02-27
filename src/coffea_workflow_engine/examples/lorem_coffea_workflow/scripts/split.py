from pathlib import Path

from pathlib import Path
import json

def split_file(src, chunk_bytes, dir, manifest_path):
    src_path = Path(src)
    out_dir = Path(dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    data = src_path.read_bytes()
    n_chunks = (len(data) + chunk_bytes - 1) // chunk_bytes

    chunk_ids = [f"{i:03d}" for i in range(n_chunks)]
    chunk_files = []

    for i, cid in enumerate(chunk_ids):
        start = i * chunk_bytes
        end = start + chunk_bytes
        out_file = out_dir / f"chunk_{cid}.txt"
        out_file.write_bytes(data[start:end])
        chunk_files.append(str(out_file))

    manifest = {
        "src": str(src_path),
        "chunk_bytes": chunk_bytes,
        "chunks_dir": str(out_dir),
        "n_chunks": n_chunks,
        "chunk_ids": chunk_ids,
        "chunk_files": chunk_files,
    }
    Path(manifest_path).write_text(json.dumps(manifest, indent=2))
    