from pathlib import Path

Path(snakemake.output[0]).write_text(snakemake.params.msg + "\n", encoding="utf-8")