from __future__ import annotations

import json
from pathlib import Path

USC_JSON = Path("results/bench_loghub_all.json")
CLP_JSON = Path("results/clp/clp_bench.json")
CLP_SEARCH_JSON = Path("results/clp/clp_search.json")

OUT = Path("results/competitor_report.md")

DATASETS = ["Android", "Apache", "BGL", "HDFS", "Zookeeper"]

def fmt_bytes(n: int) -> str:
    x = float(n)
    for unit in ["B", "KB", "MB", "GB"]:
        if x < 1024 or unit == "GB":
            return f"{x:.2f} {unit}" if unit != "B" else f"{int(x)} B"
        x /= 1024
    return f"{x:.2f} GB"

def ratio_cell(r: float) -> str:
    return f"{r:.2f}×"

def get_usc_ratio(usc: dict, ds: str, key: str) -> float | None:
    if ds not in usc: return None
    if key not in usc[ds]: return None
    return float(usc[ds][key].get("ratio", 0.0))

def get_clp_ratio(clp: dict, ds: str) -> float | None:
    if ds not in clp: return None
    return float(clp[ds].get("ratio", 0.0))

def avg_clp_search(clp_search: dict, ds: str) -> float | None:
    if ds not in clp_search: return None
    times = []
    for q, obj in clp_search[ds].items():
        t = obj.get("search_s", None)
        if t is not None:
            times.append(float(t))
    if not times:
        return None
    return sum(times) / len(times)

def main():
    usc = json.loads(USC_JSON.read_text(encoding="utf-8"))
    clp = json.loads(CLP_JSON.read_text(encoding="utf-8"))
    clp_search = json.loads(CLP_SEARCH_JSON.read_text(encoding="utf-8")) if CLP_SEARCH_JSON.exists() else {}

    lines = []
    lines.append("# USC vs CLP — Competitor Report (200k lines)\n")
    lines.append("This report compares USC to CLP on the same 5 LogHub datasets (200,000 lines each).")
    lines.append("")

    # --- Compression table ---
    lines.append("## Compression ratio (higher is better)\n")
    lines.append("| Dataset | CLP | USC-hot-lite-full | USC-cold | Winner (Queryable) | Winner (Max) |")
    lines.append("|---|---:|---:|---:|---|---|")

    for ds in DATASETS:
        clp_r = get_clp_ratio(clp, ds)
        usc_q = get_usc_ratio(usc, ds, "USC-hot-lite-full")
        usc_c = get_usc_ratio(usc, ds, "USC-cold")

        clp_s = ratio_cell(clp_r) if clp_r else "—"
        usc_qs = ratio_cell(usc_q) if usc_q else "—"
        usc_cs = ratio_cell(usc_c) if usc_c else "—"

        # winner queryable = compare CLP vs USC-hot-lite-full
        winner_q = "—"
        if clp_r and usc_q:
            winner_q = "USC" if usc_q > clp_r else "CLP"

        # winner max = compare CLP vs USC-cold
        winner_m = "—"
        if clp_r and usc_c:
            winner_m = "USC" if usc_c > clp_r else "CLP"

        lines.append(f"| {ds} | {clp_s} | {usc_qs} | {usc_cs} | {winner_q} | {winner_m} |")

    lines.append("")
    lines.append("## CLP search time (lower is better)\n")
    lines.append("Average time over 4 queries: `ERROR`, `Exception`, `WARN`, `INFO`.")
    lines.append("")
    lines.append("| Dataset | Avg CLP search time |")
    lines.append("|---|---:|")

    for ds in DATASETS:
        t = avg_clp_search(clp_search, ds)
        if t is None:
            lines.append(f"| {ds} | — |")
        else:
            lines.append(f"| {ds} | {t:.3f}s |")

    OUT.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("✅ wrote:", OUT)

if __name__ == "__main__":
    main()
