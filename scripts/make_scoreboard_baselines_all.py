from __future__ import annotations
import json
from pathlib import Path

INP = Path("results/baselines_all_gzip_zstd.json")
OUT = Path("results/scoreboard_baselines_all.md")

def main():
    d = json.loads(INP.read_text(encoding="utf-8"))
    rows = []
    for ds, info in d.items():
        rows.append((ds, float(info["gzip_ratio"]), float(info["zstd_ratio"]), int(info["raw_size"])))
    rows.sort(key=lambda x: x[0].lower())

    lines = []
    lines.append("# Baselines Scoreboard — LogHub (200k lines)\n")
    lines.append("| Dataset | Raw bytes | gzip-9 | zstd-19 |")
    lines.append("|---|---:|---:|---:|")
    for ds, gz, zs, raw in rows:
        lines.append(f"| {ds} | {raw:,} | {gz:.2f}× | {zs:.2f}× |")

    OUT.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"✅ wrote: {OUT}")

if __name__ == "__main__":
    main()
