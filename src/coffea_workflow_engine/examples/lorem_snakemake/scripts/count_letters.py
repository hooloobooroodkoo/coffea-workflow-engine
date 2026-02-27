import json
from collections import Counter
from pathlib import Path
import string

text = Path(snakemake.input[0]).read_text(encoding="utf-8", errors="ignore").lower()


cnt = Counter(ch for ch in text if ch in string.ascii_lowercase)

out = {ch: int(cnt.get(ch, 0)) for ch in string.ascii_lowercase}

Path(snakemake.output[0]).write_text(json.dumps(out, indent=2, sort_keys=True) + "\n", encoding="utf-8")