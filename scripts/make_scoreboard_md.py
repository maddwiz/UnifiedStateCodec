from __future__ import annotations

import json
from pathlib import Path

INP_USC = Path("results/bench_loghub_all.json")
INP_CLP = Path("results/clp/clp_bench.json")
OUT = Path("results/scoreboard_200k.md")

def fmt_bytes(n: int) -> str:
    x = float(n)
    for unit in ["B", "KB", "MB", "GB"]:
        if x < 1024 or unit == "GB":
            return f"{x:.2f} {unit}" if unit != "B" else f"{int(x)} B"
        x /= 1024
    return f"{x:.2f} GB"

def cell(row: dict, name: str) -> str:
    if name not in row:
        return "—"
    d = row[name]
    if not isinstance(d, dict):
        return "—"
    b = d.get("bytes", None)
    r = d.get("ratio", None)
    if b is None or r is None:
        return "—"
    return f"{fmt_bytes(int(b))} ({float(r):.2f}×)"

def raw_cell(row: dict) -> str:
    # Try multiple possible raw keys
    for k in ["RAW", "raw"]:
        if k in row and isinstance(row[k], dict) and "bytes" in row[k]:
            return fmt_bytes(int(row[k]["bytes"]))
    return "—"

def clp_cell(clp: dict, ds: str) -> str:
    if ds not in clp:
        return "—"
    b = clp[ds].get("bytes", None)
    r = clp[ds].get("ratio", None)
    if b is None or r is None:
        return "—"
    return f"{fmt_bytes(int(b))} ({float(r):.2f}×)"

def main():
    usc = json.loads(INP_USC.read_text(encoding="utf-8"))
    clp = {}
    if INP_CLP.exists():
        clp = json.loads(INP_CLP.read_text(encoding="utf-8"))

    datasets = sorted(usc.keys())

    lines = []
    lines.append("# USC Scoreboard (200k lines)\n")
    lines.append("| Dataset | RAW | gzip | zstd-19 | CLP | USC-stream | USC-hot-lite | USC-hot-lite-full | USC-hot | USC-cold |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")

    for ds in datasets:
        row = usc[ds]
        raw = raw_cell(row)

        gzip = cell(row, "gzip")
        zstd = cell(row, "zstd-19")
        clp_c = clp_cell(clp, ds)

        usc_stream = cell(row, "USC-stream")
        usc_hotlite = cell(row, "USC-hot-lite")
        usc_hotlite_full = cell(row, "USC-hot-lite-full")
        usc_hot = cell(row, "USC-hot")
        usc_cold = cell(row, "USC-cold")

        lines.append(
            f"| {ds} | {raw} | {gzip} | {zstd} | {clp_c} | {usc_stream} | {usc_hotlite} | {usc_hotlite_full} | {usc_hot} | {usc_cold} |"
        )

    OUT.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("✅ wrote:", OUT)

if __name__ == "__main__":
    main()
