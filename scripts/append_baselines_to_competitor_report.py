from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "results"

REPORT = RESULTS / "competitor_report.md"
BASELINES = RESULTS / "baselines_gzip_zstd.json"

if not REPORT.exists():
    raise SystemExit("❌ competitor_report.md not found. Run make_competitor_report_md.py first.")

if not BASELINES.exists():
    raise SystemExit("❌ baselines_gzip_zstd.json not found. Run bench_baselines_gzip_zstd.py first.")

data = json.loads(BASELINES.read_text(encoding="utf-8"))

rows = []
for ds, info in data.items():
    if not info.get("ok", False):
        continue
    gzip_ratio = info.get("gzip_ratio", 0.0)
    zstd_ratio = info.get("zstd_ratio", 0.0)
    raw_size = info.get("raw_size", 0)
    rows.append((ds, raw_size, gzip_ratio, zstd_ratio))

rows.sort(key=lambda x: x[0].lower())

section = []
section.append("\n## Baseline compression ratios (gzip-9 / zstd-19)\n")
section.append("These are general-purpose baselines on the *same exact 200k raw logs*.\n")
section.append("\n| Dataset | Raw size | gzip-9 | zstd-19 |\n")
section.append("|---|---:|---:|---:|\n")

for ds, raw_size, gz, zs in rows:
    section.append(f"| {ds} | {raw_size:,} B | {gz:.2f}× | {zs:.2f}× |\n")

txt = REPORT.read_text(encoding="utf-8")

# Prevent duplicates
if "Baseline compression ratios (gzip-9 / zstd-19)" in txt:
    # Replace old baseline section (simple strategy: truncate at baseline header and rewrite)
    head = txt.split("## Baseline compression ratios (gzip-9 / zstd-19)")[0].rstrip()
    txt2 = head + "".join(section)
else:
    txt2 = txt.rstrip() + "".join(section)

REPORT.write_text(txt2 + "\n", encoding="utf-8")
print("✅ appended gzip/zstd baselines into results/competitor_report.md")
