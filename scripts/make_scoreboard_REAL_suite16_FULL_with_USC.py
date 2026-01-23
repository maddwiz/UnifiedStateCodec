from __future__ import annotations

import json
from pathlib import Path

BASE = Path("results/baselines_real_suite16_200k_extended.json")
USC  = Path("results/bench_usc_real_suite16_all_modes_200k.json")
OUT  = Path("results/scoreboard_REAL_suite16_FULL_200k.md")

def g(d: dict, *keys, default=0.0):
    cur = d
    for k in keys:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur

def main():
    if not BASE.exists():
        raise SystemExit("❌ missing baselines_real_suite16_200k_extended.json")
    if not USC.exists():
        raise SystemExit("❌ missing bench_usc_real_suite16_all_modes_200k.json (run USC bench script first)")

    base = json.loads(BASE.read_text(encoding="utf-8"))
    usc  = json.loads(USC.read_text(encoding="utf-8"))

    datasets = sorted(base.keys(), key=lambda x: x.lower())

    lines = []
    lines.append("# REAL LogHub Suite16 @200k — FULL Scoreboard (Competitors + USC)\n")
    lines.append("All datasets are from the **real Zenodo LogHub v8** extracts.\n")
    lines.append("| Dataset | gzip× | zstd× | brotli× | xz× | USC-stream× | USC-hot× | USC-hot-lite× | USC-hot-lite-full× | USC-cold× |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")

    for ds in datasets:
        lines.append(
            f"| {ds} | "
            f"{g(base,ds,'gzip','ratio'):.2f}× | "
            f"{g(base,ds,'zstd','ratio'):.2f}× | "
            f"{g(base,ds,'brotli','ratio'):.2f}× | "
            f"{g(base,ds,'xz','ratio'):.2f}× | "
            f"{g(usc,ds,'stream','ratio'):.2f}× | "
            f"{g(usc,ds,'hot','ratio'):.2f}× | "
            f"{g(usc,ds,'hot-lite','ratio'):.2f}× | "
            f"{g(usc,ds,'hot-lite-full','ratio'):.2f}× | "
            f"{g(usc,ds,'cold','ratio'):.2f}× |"
        )

    OUT.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"✅ wrote: {OUT}")

if __name__ == "__main__":
    main()
