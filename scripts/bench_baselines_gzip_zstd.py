from __future__ import annotations

import gzip
import json
import subprocess
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "results"
OUT = RESULTS / "baselines_gzip_zstd.json"

DATASETS = ["Android", "Apache", "BGL", "HDFS", "Zookeeper"]
N_LINES = 200_000

def run(cmd: list[str]) -> tuple[int, str, str, float]:
    t0 = time.time()
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    dt = time.time() - t0
    return p.returncode, p.stdout, p.stderr, dt

def main():
    report: dict[str, dict] = {}

    for ds in DATASETS:
        raw = RESULTS / f"__raw_{ds}_{N_LINES}.log"
        report[ds] = {"raw_path": str(raw)}

        if not raw.exists() or raw.stat().st_size == 0:
            report[ds]["ok"] = False
            report[ds]["error"] = "raw file missing"
            print(f"❌ {ds}: missing {raw.name}")
            continue

        raw_bytes = raw.read_bytes()
        raw_size = len(raw_bytes)

        # gzip -9
        gz_bytes = gzip.compress(raw_bytes, compresslevel=9)
        gz_size = len(gz_bytes)

        # zstd -19
        tmp_zst = RESULTS / f"__raw_{ds}_{N_LINES}.log.zst"
        rc, out, err, dt = run(["zstd", "-19", "-q", "-f", "-o", str(tmp_zst), str(raw)])
        if rc != 0 or not tmp_zst.exists():
            report[ds]["ok"] = False
            report[ds]["error"] = f"zstd failed rc={rc} err={err.strip()}"
            print(f"❌ {ds}: zstd failed")
            continue

        zst_size = tmp_zst.stat().st_size

        report[ds]["ok"] = True
        report[ds]["raw_size"] = raw_size
        report[ds]["gzip_size"] = gz_size
        report[ds]["zstd_size"] = zst_size
        report[ds]["gzip_ratio"] = (raw_size / gz_size) if gz_size > 0 else 0.0
        report[ds]["zstd_ratio"] = (raw_size / zst_size) if zst_size > 0 else 0.0

        print(
            f"✅ {ds:<10} raw={raw_size:>10}B  gzip={gz_size:>10}B ({report[ds]['gzip_ratio']:.2f}×)"
            f"  zstd={zst_size:>10}B ({report[ds]['zstd_ratio']:.2f}×)"
        )

    OUT.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"\n✅ wrote: {OUT}")

if __name__ == "__main__":
    main()
