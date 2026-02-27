from pathlib import Path

chunk_len = int(snakemake.params.chunk_len)
data = Path(snakemake.input[0]).read_bytes()

for i, out_path in enumerate(snakemake.output):
    start = i * chunk_len
    end = start + chunk_len
    Path(out_path).write_bytes(data[start:end])