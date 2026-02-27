import json
from collections import Counter
from pathlib import Path
import string

total = Counter({ch: 0 for ch in string.ascii_lowercase})

for fname in snakemake.input:
    part = json.loads(Path(fname).read_text(encoding="utf-8"))
    total.update({k: int(v) for k, v in part.items()})

out = {ch: int(total[ch]) for ch in string.ascii_lowercase}
Path(snakemake.output[0]).write_text(json.dumps(out, indent=2, sort_keys=True) + "\n", encoding="utf-8")