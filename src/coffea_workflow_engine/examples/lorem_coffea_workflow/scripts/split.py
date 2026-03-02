from pathlib import Path

def split_file(src: str, chunk_bytes: int, out_dir: str, chunk_files: str = "chunk_{i:03d}.txt"):
    data = Path(src).read_bytes()
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    n_chunks = (len(data) + chunk_bytes - 1) // chunk_bytes

    chunk_paths = []
    chunk_ids = []

    for i in range(n_chunks):
        cid = f"{i:03d}"
        fname = chunk_files.format(i=i, cid=cid)
        p = out / fname

        start = i * chunk_bytes
        end = start + chunk_bytes
        p.write_bytes(data[start:end])

        chunk_paths.append(str(p))
        chunk_ids.append(cid)

    return {
        "out_dir": str(out),
        "n_chunks": n_chunks,
        "chunk_ids": chunk_ids,
        "chunk_files": chunk_paths,
    }