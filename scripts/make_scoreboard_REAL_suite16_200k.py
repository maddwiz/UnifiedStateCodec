from __future__ import annotations

import json
from pathlib import Path

BASE = Path("results/baselines_real_suite16_200k_extended.json")
OUT = Path("results/scoreboard_REAL_suite16_200k.md")

def main():
    if not BASE.exists():
        raise SystemExit("❌ missing results/baselines_real_suite16_200k_extended.json")

    comp = json.loads(BASE.read_text(encoding="utf-8"))

    # For now, we at least publish competitor baseline scoreboard.
    # (Your USC benches will have their own jsons; we'll merge them next once we see filenames.)
    lines = []
    lines.append("# REAL LogHub Suite16 @200k — Competitor Baselines\n")
    lines.append("These are the **true LogHub v8 datasets** (not cycled).")
    lines.append("")
    lines.append("| Dataset | Raw MB | gzip× | zstd× | brotli× | xz× | lz4× | bzip2× |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|")

    def get_ratio(ds: str, k: str) -> float:
        return float(comp[ds].get(k, {}).get("ratio", 0.0))

    rows = []
    for ds in sorted(comp.keys(), key=lambda x: x.lower()):
        raw_mb = float(comp[ds]["raw_size"]) / (1024 * 1024)
        rows.append((ds, raw_mb))

    for ds, raw_mb in rows:
        lines.append(
            f"| {ds} | {raw_mb:.2f} | "
            f"{get_ratio(ds,'gzip'):.2f}× | {get_ratio(ds,'zstd'):.2f}× | "
            f"{get_ratio(ds,'brotli'):.2f}× | {get_ratio(ds,'xz'):.2f}× | "
            f"{get_ratio(ds,'lz4'):.2f}× | {get_ratio(ds,'bzip2'):.2f}× |"
        )

    OUT.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"✅ wrote: {OUT}")

if __name__ == "__main__":
    main()
