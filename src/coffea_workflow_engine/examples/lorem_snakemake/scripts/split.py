from pathlib import Path

inp = Path(snakemake.input[0])
data = inp.read_bytes()

chunk_bytes = int(snakemake.params.chunk_bytes)

for i, out_name in enumerate(snakemake.output):
    start = i * chunk_bytes
    end = start + chunk_bytes
    Path(out_name).write_bytes(data[start:end])