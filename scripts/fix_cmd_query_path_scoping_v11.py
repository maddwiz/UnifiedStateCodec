from __future__ import annotations
from pathlib import Path
import re

APP = Path("src/usc/cli/app.py")
s = APP.read_text(encoding="utf-8", errors="ignore")
lines = s.splitlines(True)

# find cmd_query
start = None
for i, ln in enumerate(lines):
    if ln.lstrip().startswith("def cmd_query"):
        start = i
        break
if start is None:
    raise SystemExit("❌ cmd_query not found")

# find end of cmd_query by next top-level def
end = len(lines)
for j in range(start + 1, len(lines)):
    if lines[j].startswith("def ") and not lines[j].startswith("def cmd_query"):
        end = j
        break

chunk = lines[start:end]
chunk_txt = "".join(chunk)

# 1) Remove any inner "from pathlib import Path" inside cmd_query
chunk_txt2 = re.sub(r"(?m)^\s*from\s+pathlib\s+import\s+Path\s*\n", "", chunk_txt)

# 2) Ensure we have "import pathlib" near top of cmd_query (after def line)
if "import pathlib" not in chunk_txt2:
    # insert right after first line (def ...)
    parts = chunk_txt2.splitlines(True)
    parts.insert(1, "    import pathlib\n")
    chunk_txt2 = "".join(parts)

# 3) Replace Path(...) with pathlib.Path(...) inside cmd_query
# (only inside cmd_query region)
chunk_txt2 = re.sub(r"\bPath\(", "pathlib.Path(", chunk_txt2)

# write back
new_lines = lines[:start] + chunk_txt2.splitlines(True) + lines[end:]
APP.write_text("".join(new_lines), encoding="utf-8")
print("✅ fixed Path scoping in cmd_query (use pathlib.Path)")
