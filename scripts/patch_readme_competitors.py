from __future__ import annotations
from pathlib import Path

READ = Path("README.md")
REP = Path("results/competitor_report.md")

start = "<!-- USC_COMPETITOR_START -->"
end   = "<!-- USC_COMPETITOR_END -->"

readme = READ.read_text(encoding="utf-8", errors="ignore")
report = REP.read_text(encoding="utf-8", errors="ignore")

block = f"{start}\n\n{report}\n{end}\n"

if start in readme and end in readme:
    pre = readme.split(start)[0]
    post = readme.split(end)[1]
    new = pre + block + post
else:
    new = readme + "\n\n" + block

READ.write_text(new, encoding="utf-8")
print("✅ README updated with competitor report block")
