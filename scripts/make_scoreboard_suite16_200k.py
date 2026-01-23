from __future__ import annotations
from pathlib import Path
import json

RAW_DIR = Path("results/raw_real_suite16_200k")
OUT_DIR = Path("results/suite16_200k_competitors")
REPORT_MD = Path("results/scoreboard_suite16_200k.md")
REPORT_JSON = Path("results/bench_suite16_200k.json")

DATASETS = [
    "Android_v2","Apache","BGL","HDFS_v2","HPC","Hadoop","HealthApp","Linux",
    "Mac","OpenStack","Proxifier","SSH","Spark","Thunderbird","Windows","Zookeeper"
]

def sz(p: Path) -> int:
    return p.stat().st_size if p.exists() else 0

def ratio(raw: int, comp: int) -> float:
    return (raw / comp) if (raw > 0 and comp > 0) else 0.0

def fmt_mb_and_ratio(comp_bytes: int, r: float) -> str:
    if comp_bytes <= 0:
        return "—"
    return f"{comp_bytes/1e6:.2f} ({r:.2f}x)"

rows = []
for name in DATASETS:
    raw = RAW_DIR / f"{name}_200000.log"
    if not raw.exists():
        continue

    raw_bytes = sz(raw)
    d = {
        "dataset": name,
        "raw_bytes": raw_bytes,
        "usc_hot_bytes": sz(OUT_DIR / f"{name}_hot.bin"),
        "usc_hotlitefull_bytes": sz(OUT_DIR / f"{name}_hot-lite-full.bin"),
        "usc_cold_bytes": sz(OUT_DIR / f"{name}_cold.bin"),
        "gzip_bytes": sz(OUT_DIR / f"{name}.gz"),
        "zstd_bytes": sz(OUT_DIR / f"{name}.zst"),
        "zstd_dict_bytes": sz(OUT_DIR / f"{name}.zst_dict"),
    }

    d["usc_hot_ratio"] = ratio(raw_bytes, d["usc_hot_bytes"])
    d["usc_hotlitefull_ratio"] = ratio(raw_bytes, d["usc_hotlitefull_bytes"])
    d["usc_cold_ratio"] = ratio(raw_bytes, d["usc_cold_bytes"])
    d["gzip_ratio"] = ratio(raw_bytes, d["gzip_bytes"])
    d["zstd_ratio"] = ratio(raw_bytes, d["zstd_bytes"])
    d["zstd_dict_ratio"] = ratio(raw_bytes, d["zstd_dict_bytes"])
    rows.append(d)

REPORT_JSON.write_text(json.dumps(rows, indent=2))

lines = []
lines.append("# Suite16 @ 200k — Full Scoreboard\n")
lines.append("All sizes are MB. Ratio = RAW / COMPRESSED.\n")
lines.append("| Dataset | RAW MB | USC HOT | USC HOT-LITE-FULL | USC COLD | gzip | zstd | zstd+dict |")
lines.append("|---|---:|---:|---:|---:|---:|---:|---:|")

for r in rows:
    raw_mb = r["raw_bytes"] / 1e6
    lines.append(
        f"| {r['dataset']} | {raw_mb:.2f} | "
        f"{fmt_mb_and_ratio(r['usc_hot_bytes'], r['usc_hot_ratio'])} | "
        f"{fmt_mb_and_ratio(r['usc_hotlitefull_bytes'], r['usc_hotlitefull_ratio'])} | "
        f"{fmt_mb_and_ratio(r['usc_cold_bytes'], r['usc_cold_ratio'])} | "
        f"{fmt_mb_and_ratio(r['gzip_bytes'], r['gzip_ratio'])} | "
        f"{fmt_mb_and_ratio(r['zstd_bytes'], r['zstd_ratio'])} | "
        f"{fmt_mb_and_ratio(r['zstd_dict_bytes'], r['zstd_dict_ratio'])} |"
    )

REPORT_MD.write_text("\n".join(lines))
print("WROTE:", REPORT_MD)
print("WROTE:", REPORT_JSON)
