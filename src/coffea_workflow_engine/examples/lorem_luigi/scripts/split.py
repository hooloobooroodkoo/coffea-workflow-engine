from pathlib import Path

def split_file(src, outputs, chunk_bytes):
    data = Path(src).read_bytes()
    for i, out_name in enumerate(outputs):
        start = i * chunk_bytes
        end = start + chunk_bytes
        Path(out_name).write_bytes(data[start:end])