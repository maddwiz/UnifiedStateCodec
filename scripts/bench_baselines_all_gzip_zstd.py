from __future__ import annotations

import gzip
import json
import subprocess
import time
from pathlib import Path

RAW_DIR = Path("results/raw_all_200k")
OUT = Path("results/baselines_all_gzip_zstd.json")

def run(cmd: list[str]) -> tuple[int, str, str, float]:
    t0 = time.time()
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return p.returncode, p.stdout, p.stderr, (time.time() - t0)

def main():
    if not RAW_DIR.exists():
        raise SystemExit("❌ results/raw_all_200k not found")

    logs = sorted(RAW_DIR.glob("*_200k.log"))
    if not logs:
        raise SystemExit("❌ no *_200k.log files found in results/raw_all_200k")

    report = {}

    for raw in logs:
        ds = raw.name.replace("_200k.log", "")
        raw_bytes = raw.read_bytes()
        raw_size = len(raw_bytes)

        gz = gzip.compress(raw_bytes, compresslevel=9)
        gz_size = len(gz)

        zst_path = Path(f"results/raw_all_200k/{ds}_200k.log.zst")
        rc, out, err, dt = run(["zstd", "-19", "-q", "-f", "-o", str(zst_path), str(raw)])
        if rc != 0 or not zst_path.exists():
            print(f"❌ zstd failed for {ds}: {err.strip()}")
            continue

        zst_size = zst_path.stat().st_size

        report[ds] = {
            "raw_path": str(raw),
            "raw_size": raw_size,
            "gzip_size": gz_size,
            "zstd_size": zst_size,
            "gzip_ratio": raw_size / gz_size if gz_size else 0.0,
            "zstd_ratio": raw_size / zst_size if zst_size else 0.0,
        }

        print(f"✅ {ds:<12} gzip={report[ds]['gzip_ratio']:.2f}×  zstd={report[ds]['zstd_ratio']:.2f}×")

    OUT.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"\n✅ wrote: {OUT}")

if __name__ == "__main__":
    main()
