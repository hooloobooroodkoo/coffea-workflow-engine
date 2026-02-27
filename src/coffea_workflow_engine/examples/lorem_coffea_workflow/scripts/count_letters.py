import json
import string
from collections import Counter
from pathlib import Path

def count_letters_file(input, out):
    text = Path(input).read_text(encoding="utf-8", errors="ignore").lower()
    cnt = Counter(ch for ch in text if ch in string.ascii_lowercase)
    output = {ch: int(cnt.get(ch, 0)) for ch in string.ascii_lowercase}
    Path(out).write_text(json.dumps(output, indent=2, sort_keys=True) + "\n", encoding="utf-8")