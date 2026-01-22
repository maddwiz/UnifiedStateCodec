from __future__ import annotations

import json
from pathlib import Path

INP = Path("results/bench_loghub_all.json")
OUT = Path("results/scoreboard_200k.md")

def fmt_bytes(n: int) -> str:
    x = float(n)
    for unit in ["B", "KB", "MB", "GB"]:
        if x < 1024 or unit == "GB":
            return f"{x:.2f} {unit}" if unit != "B" else f"{int(x)} B"
        x /= 1024
    return f"{x:.2f} GB"

def pick_raw(row: dict) -> str:
    # Bench JSON may store raw size under different keys.
    for k in ["RAW", "raw", "raw_bytes", "raw_size"]:
        if k in row and isinstance(row[k], dict):
            b = row[k].get("bytes", None)
            if b is not None:
                return fmt_bytes(int(b))
    # fallback: look for a direct bytes field
    for k in ["RAW", "raw", "raw_bytes", "raw_size"]:
        if k in row and isinstance(row[k], (int, float)):
            return fmt_bytes(int(row[k]))
    return "—"

def pick_cell(row: dict, name: str) -> str:
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

def main():
    obj = json.loads(INP.read_text(encoding="utf-8"))
    datasets = sorted(obj.keys())

    lines = []
    lines.append("# USC Scoreboard (200k lines)\n")
    lines.append("| Dataset | RAW | gzip | zstd-19 | USC-stream | USC-hot-lite | USC-hot-lite-full | USC-hot | USC-cold |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|")

    for ds in datasets:
        row = obj[ds]
        raw = pick_raw(row)
        gzip = pick_cell(row, "gzip")
        zstd = pick_cell(row, "zstd-19")
        usc_stream = pick_cell(row, "USC-stream")
        usc_hotlite = pick_cell(row, "USC-hot-lite")
        usc_hotlite_full = pick_cell(row, "USC-hot-lite-full")
        usc_hot = pick_cell(row, "USC-hot")
        usc_cold = pick_cell(row, "USC-cold")
        lines.append(
            f"| {ds} | {raw} | {gzip} | {zstd} | {usc_stream} | {usc_hotlite} | {usc_hotlite_full} | {usc_hot} | {usc_cold} |"
        )

    OUT.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("✅ wrote:", OUT)

if __name__ == "__main__":
    main()
