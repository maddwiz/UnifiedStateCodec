from __future__ import annotations

import json
from pathlib import Path

INP = Path("results/bench_loghub_all.json")
OUT = Path("results/scoreboard_200k.md")

def fmt_bytes(n: int) -> str:
    # human readable (KB/MB)
    x = float(n)
    for unit in ["B", "KB", "MB", "GB"]:
        if x < 1024 or unit == "GB":
            return f"{x:.2f} {unit}" if unit != "B" else f"{int(x)} B"
        x /= 1024
    return f"{x:.2f} GB"

def main():
    obj = json.loads(INP.read_text(encoding="utf-8"))
    # expected structure: obj[dset][method] = {"bytes":..., "ratio":..., ...}
    # We'll robustly handle both nested or list formats.
    lines = []
    lines.append("# USC Scoreboard (200k lines)\n")
    lines.append("| Dataset | RAW | gzip | zstd-19 | USC-stream | USC-hot-lite | USC-hot-lite-full | USC-hot | USC-cold |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|")

    # try to detect datasets
    datasets = sorted(obj.keys())

    for ds in datasets:
        row = obj[ds]
        def cell(name: str) -> str:
            if name not in row:
                return "—"
            b = row[name].get("bytes", None)
            r = row[name].get("ratio", None)
            if b is None or r is None:
                return "—"
            return f"{fmt_bytes(int(b))} ({float(r):.2f}×)"

        raw = cell("RAW")
        gzip = cell("gzip")
        zstd = cell("zstd-19")
        usc_stream = cell("USC-stream")
        usc_hotlite = cell("USC-hot-lite")
        usc_hotlite_full = cell("USC-hot-lite-full")
        usc_hot = cell("USC-hot")
        usc_cold = cell("USC-cold")

        lines.append(
            f"| {ds} | {raw} | {gzip} | {zstd} | {usc_stream} | {usc_hotlite} | {usc_hotlite_full} | {usc_hot} | {usc_cold} |"
        )

    OUT.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("✅ wrote:", OUT)

if __name__ == "__main__":
    main()
