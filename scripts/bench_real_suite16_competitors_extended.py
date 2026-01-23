from __future__ import annotations

import gzip
import json
import shutil
import subprocess
import time
from pathlib import Path

RAW_DIR = Path("results/raw_real_suite16_200k")
OUT = Path("results/baselines_real_suite16_200k_extended.json")

TOOLS = ["zstd", "brotli", "xz", "lz4", "bzip2"]

def have(tool: str) -> bool:
    return shutil.which(tool) is not None

def run(cmd: list[str], inp: bytes | None = None) -> tuple[int, bytes, bytes, float]:
    t0 = time.time()
    p = subprocess.run(cmd, input=inp, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return p.returncode, p.stdout, p.stderr, (time.time() - t0)

def ratio(raw_size: int, comp_size: int) -> float:
    return (raw_size / comp_size) if comp_size > 0 else 0.0

def main():
    if not RAW_DIR.exists():
        raise SystemExit("❌ results/raw_real_suite16_200k not found.")

    logs = sorted(RAW_DIR.glob("*_200000.log"))
    if not logs:
        raise SystemExit("❌ no *_200000.log files found")

    print("=== tools ===")
    for t in TOOLS:
        print(f"{t:<7} {'✅' if have(t) else '❌'}")

    report: dict[str, dict] = {}

    for raw in logs:
        ds = raw.name.replace("_200000.log", "")
        raw_bytes = raw.read_bytes()
        raw_size = len(raw_bytes)

        row = {"raw_size": raw_size}

        # gzip -9
        t0 = time.time()
        gz = gzip.compress(raw_bytes, compresslevel=9)
        gz_t = time.time() - t0
        row["gzip"] = {"size": len(gz), "ratio": ratio(raw_size, len(gz)), "enc_s": gz_t}

        # zstd -19
        if have("zstd"):
            rc, out, err, t = run(["zstd", "-19", "-q", "-c"], inp=raw_bytes)
            row["zstd"] = {"size": len(out), "ratio": ratio(raw_size, len(out)), "enc_s": t}

        # brotli -11
        if have("brotli"):
            rc, out, err, t = run(["brotli", "-q", "11", "-c"], inp=raw_bytes)
            row["brotli"] = {"size": len(out), "ratio": ratio(raw_size, len(out)), "enc_s": t}

        # xz -9
        if have("xz"):
            rc, out, err, t = run(["bash", "-lc", "xz -9 -c"], inp=raw_bytes)
            row["xz"] = {"size": len(out), "ratio": ratio(raw_size, len(out)), "enc_s": t}

        # bzip2 -9
        if have("bzip2"):
            rc, out, err, t = run(["bash", "-lc", "bzip2 -9 -c"], inp=raw_bytes)
            row["bzip2"] = {"size": len(out), "ratio": ratio(raw_size, len(out)), "enc_s": t}

        # lz4 -9
        if have("lz4"):
            rc, out, err, t = run(["lz4", "-9", "-c"], inp=raw_bytes)
            row["lz4"] = {"size": len(out), "ratio": ratio(raw_size, len(out)), "enc_s": t}

        report[ds] = row

        def getr(k): 
            return report[ds].get(k, {}).get("ratio", 0.0)

        print(f"✅ {ds:<12} gzip={getr('gzip'):.2f}× zstd={getr('zstd'):.2f}× br={getr('brotli'):.2f}× xz={getr('xz'):.2f}×")

    OUT.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"\n✅ wrote: {OUT}")

if __name__ == "__main__":
    main()
