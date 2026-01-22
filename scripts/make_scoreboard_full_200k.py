from __future__ import annotations

import re
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "results"
OUT = RESULTS / "scoreboard_full_200k.md"

DATASETS = ["Android", "Apache", "BGL", "HDFS", "Zookeeper"]
QUERIES = ["ERROR", "Exception", "WARN", "INFO"]

COMP_REPORT = RESULTS / "competitor_report.md"
BASELINES = RESULTS / "baselines_gzip_zstd.json"
USC_Q = RESULTS / "usc_query_speed.json"


def load_json(p: Path) -> dict:
    if not p.exists():
        return {}
    return json.loads(p.read_text(encoding="utf-8"))


def fmt_ratio(x) -> str:
    try:
        x = float(x)
    except Exception:
        return "-"
    if x <= 0:
        return "-"
    return f"{x:.2f}×"


def fmt_s(x) -> str:
    try:
        x = float(x)
    except Exception:
        return "-"
    if x <= 0:
        return "-"
    return f"{x:.3f}s"


def avg(xs: list[float]) -> float | None:
    xs = [x for x in xs if isinstance(x, (int, float)) and x > 0]
    if not xs:
        return None
    return sum(xs) / len(xs)


def parse_competitor_report(md: str) -> tuple[dict, dict]:
    """
    Returns:
      ratios[dataset] = {clp, usc_hot, usc_cold}
      clp_times[dataset] = avg_s
    """
    ratios: dict[str, dict] = {}
    clp_times: dict[str, float] = {}

    # --- parse ratio table ---
    # lines like: | Android | 15.96× | 11.78× | 21.15× | CLP | USC |
    ratio_re = re.compile(
        r"^\|\s*(?P<ds>[A-Za-z]+)\s*\|\s*(?P<clp>[\d\.]+)×\s*\|\s*(?P<hot>[\d\.]+)×\s*\|\s*(?P<cold>[\d\.]+)×\s*\|",
        re.M,
    )
    for m in ratio_re.finditer(md):
        ds = m.group("ds")
        ratios[ds] = {
            "clp": float(m.group("clp")),
            "usc_hot": float(m.group("hot")),
            "usc_cold": float(m.group("cold")),
        }

    # --- parse CLP search time table ---
    # lines like: | Android | 0.627s |
    time_re = re.compile(r"^\|\s*(?P<ds>[A-Za-z]+)\s*\|\s*(?P<t>[\d\.]+)s\s*\|\s*$", re.M)
    for m in time_re.finditer(md):
        ds = m.group("ds")
        t = float(m.group("t"))
        # Only keep the datasets we care about (avoid grabbing other tables accidentally)
        if ds in DATASETS:
            clp_times[ds] = t

    return ratios, clp_times


def main():
    if not COMP_REPORT.exists():
        raise SystemExit("❌ results/competitor_report.md missing. Run: python3 scripts/make_competitor_report_md.py")

    baselines = load_json(BASELINES)
    usc_q = load_json(USC_Q)

    md = COMP_REPORT.read_text(encoding="utf-8")
    ratios, clp_times = parse_competitor_report(md)

    lines: list[str] = []
    lines.append("# Scoreboard — 200k lines (USC vs CLP vs gzip/zstd)\n")
    lines.append("Single-table comparison on the *same exact* 200k raw logs.\n")
    lines.append("**Queryable** = supports keyword search without full decompression.\n")

    # ---------------- ratios ----------------
    lines.append("## Compression ratios (higher is better)\n")
    lines.append("| Dataset | gzip-9 | zstd-19 | CLP (Queryable) | USC-hot-lite-full (Queryable) | USC-cold (Max) | Queryable Winner | Max Winner |")
    lines.append("|---|---:|---:|---:|---:|---:|---|---|")

    for ds in DATASETS:
        gz = baselines.get(ds, {}).get("gzip_ratio")
        zs = baselines.get(ds, {}).get("zstd_ratio")

        clp = ratios.get(ds, {}).get("clp")
        hot = ratios.get(ds, {}).get("usc_hot")
        cold = ratios.get(ds, {}).get("usc_cold")

        q_winner = "-"
        if isinstance(clp, (int, float)) and isinstance(hot, (int, float)):
            q_winner = "USC" if hot >= clp else "CLP"

        max_winner = "-"
        if isinstance(cold, (int, float)) and cold > 0:
            max_winner = "USC"

        lines.append(
            f"| {ds} | {fmt_ratio(gz)} | {fmt_ratio(zs)} | {fmt_ratio(clp)} | {fmt_ratio(hot)} | {fmt_ratio(cold)} | {q_winner} | {max_winner} |"
        )

    # ---------------- query speed ----------------
    lines.append("\n## Query speed (lower is better)\n")
    lines.append("Average time over 4 queries: `ERROR`, `Exception`, `WARN`, `INFO`.\n")
    lines.append("| Dataset | CLP avg search time | USC avg search time | Speed Winner |")
    lines.append("|---|---:|---:|---|")

    for ds in DATASETS:
        clp_avg = clp_times.get(ds)

        usc_times = []
        if isinstance(usc_q.get(ds), dict):
            for q in QUERIES:
                row = usc_q[ds].get(q)
                if isinstance(row, dict) and isinstance(row.get("query_s"), (int, float)):
                    usc_times.append(float(row["query_s"]))
        usc_avg = avg(usc_times)

        winner = "-"
        if isinstance(clp_avg, (int, float)) and isinstance(usc_avg, (int, float)):
            winner = "USC" if usc_avg < clp_avg else "CLP"

        lines.append(f"| {ds} | {fmt_s(clp_avg)} | {fmt_s(usc_avg)} | {winner} |")

    OUT.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"✅ wrote: {OUT}")


if __name__ == "__main__":
    main()
