from __future__ import annotations

import json
from pathlib import Path
from statistics import mean

INP = Path("results/usc_query_speed_hot_lite_full.json")
OUT = Path("results/query_scoreboard_hot_lite_full.md")

def main():
    if not INP.exists():
        raise SystemExit(f"❌ missing {INP}")

    rows = json.loads(INP.read_text(encoding="utf-8"))

    # group by dataset
    by_ds: dict[str, list[dict]] = {}
    for r in rows:
        by_ds.setdefault(r["dataset"], []).append(r)

    lines = []
    lines.append("# USC Query Speed (hot-lite-full decode+grep) — REAL Suite16\n")
    lines.append("This benchmark measures:\n")
    lines.append("- decode time for `hot-lite-full`\n")
    lines.append("- grep time on decoded output\n")
    lines.append("- hits returned (capped)\n")
    lines.append("\n| Dataset | Avg decode (s) | Avg grep (s) | Queries |")
    lines.append("|---|---:|---:|---:|")

    for ds in sorted(by_ds.keys(), key=str.lower):
        dts = [x["decode_wall_s"] for x in by_ds[ds]]
        gts = [x["grep_wall_s"] for x in by_ds[ds]]
        lines.append(f"| {ds} | {mean(dts):.3f} | {mean(gts):.3f} | {len(by_ds[ds])} |")

    OUT.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"✅ wrote: {OUT}")

if __name__ == "__main__":
    main()
