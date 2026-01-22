from __future__ import annotations
from pathlib import Path
import re

P = Path("scripts/bench_loghub_all.py")
s = P.read_text(encoding="utf-8", errors="ignore")

# Add cold-oracle to mode lists
if "cold-oracle" not in s:
    s = re.sub(
        r'(for\s+mode\s+in\s+\[[^\]]+)\]',
        r'\1, "cold-oracle"]',
        s,
        count=1,
        flags=re.DOTALL
    )
    s = re.sub(
        r'(\bMODES\s*=\s*\[[^\]]+)\]',
        r'\1, "cold-oracle"]',
        s,
        count=1,
        flags=re.DOTALL
    )

# Ensure templates only passed for query modes OR cold-oracle
# Replace:
# if tpl_path: cmd += ["--tpl", tpl_path]
s = re.sub(
    r'if\s+tpl_path\s*:\s*\n(\s+)cmd\s*\+\=\s*\[\s*"--tpl"\s*,\s*tpl_path\s*\]\s*\n',
    r'if tpl_path and (mode in ("hot", "hot-lite", "hot-lazy", "cold-oracle")):\n\1cmd += ["--tpl", tpl_path]\n',
    s
)

P.write_text(s, encoding="utf-8")
print("✅ Bench patched: includes cold-oracle + tpl only for oracle/query modes")
