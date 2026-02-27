from pathlib import Path

data = b"".join(Path(p).read_bytes() for p in snakemake.input)
text = data.decode("utf-8").upper()

Path(snakemake.output[0]).write_text(text + "\n", encoding="utf-8")