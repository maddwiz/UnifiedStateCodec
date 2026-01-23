from __future__ import annotations

import gzip
import json
import shutil
import subprocess
import time
from pathlib import Path

RAW_DIR = Path("results/raw_loghub_full_200k")
OUT = Path("results/baselines_loghub_full_extended.json")

TOOLS = ["zstd", "brotli", "xz", "lz4", "bzip2"]

def have(tool: str) -> bool:
    return shutil.which(tool) is not None

def run(cmd: list[str]) -> tuple[int, str, str, float]:
    t0 = time.time()
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return p.returncode, p.stdout, p.stderr, (time.time() - t0)

def ratio(raw_size: int, comp_size: int) -> float:
    return (raw_size / comp_size) if comp_size > 0 else 0.0

def size_of(p: Path) -> int:
    return p.stat().st_size if p.exists() else 0

def main():
    if not RAW_DIR.exists():
        raise SystemExit("❌ results/raw_loghub_full_200k not found. Run make_raw_all_200k_from_loghub_full.sh first.")

    logs = sorted(RAW_DIR.glob("*_200000.log"))
    if not logs:
        raise SystemExit("❌ no *_200000.log files found in results/raw_loghub_full_200k")

    print("=== tools ===")
    for t in TOOLS:
        print(f"{t:<7} {'✅' if have(t) else '❌'}")

    report: dict[str, dict] = {}

    for raw in logs:
        ds = raw.name.replace("_200000.log", "")
        raw_bytes = raw.read_bytes()
        raw_size = len(raw_bytes)

        # gzip -9 (python)
        gz = gzip.compress(raw_bytes, compresslevel=9)
        gz_size = len(gz)

        # zstd -19
        zst_path = RAW_DIR / f"{ds}_200000.log.zst"
        zstd_size = 0
        if have("zstd"):
            rc, out, err, dt = run(["zstd", "-19", "-q", "-f", "-o", str(zst_path), str(raw)])
            if rc == 0:
                zstd_size = size_of(zst_path)

        # brotli -q 11
        br_path = RAW_DIR / f"{ds}_200000.log.br"
        br_size = 0
        if have("brotli"):
            rc, out, err, dt = run(["brotli", "-q", "11", "-f", "-o", str(br_path), str(raw)])
            if rc == 0:
                br_size = size_of(br_path)

        # xz -9
        xz_path = RAW_DIR / f"{ds}_200000.log.xz"
        xz_size = 0
        if have("xz"):
            rc, out, err, dt = run(["xz", "-9", "-f", "-k", str(raw)])
            # xz writes raw.xz in place when -k is set
            if rc == 0:
                # raw file becomes raw.xz next to it, so copy/move into RAW_DIR naming convention
                tmp = Path(str(raw) + ".xz")
                if tmp.exists():
                    tmp.replace(xz_path)
                    xz_size = size_of(xz_path)

        # bzip2 -9
        bz2_path = RAW_DIR / f"{ds}_200000.log.bz2"
        bz2_size = 0
        if have("bzip2"):
            rc, out, err, dt = run(["bzip2", "-9", "-f", "-k", str(raw)])
            if rc == 0:
                tmp = Path(str(raw) + ".bz2")
                if tmp.exists():
                    tmp.replace(bz2_path)
                    bz2_size = size_of(bz2_path)

        # lz4 -9
        lz4_path = RAW_DIR / f"{ds}_200000.log.lz4"
        lz4_size = 0
        if have("lz4"):
            rc, out, err, dt = run(["lz4", "-9", "-f", str(raw), str(lz4_path)])
            if rc == 0:
                lz4_size = size_of(lz4_path)

        report[ds] = {
            "raw_path": str(raw),
            "raw_size": raw_size,
            "gzip_size": gz_size,
            "zstd_size": zstd_size,
            "brotli_size": br_size,
            "xz_size": xz_size,
            "bz2_size": bz2_size,
            "lz4_size": lz4_size,
            "gzip_ratio": ratio(raw_size, gz_size),
            "zstd_ratio": ratio(raw_size, zstd_size),
            "brotli_ratio": ratio(raw_size, br_size),
            "xz_ratio": ratio(raw_size, xz_size),
            "bz2_ratio": ratio(raw_size, bz2_size),
            "lz4_ratio": ratio(raw_size, lz4_size),
        }

        print(
            f"✅ {ds:<16} "
            f"gzip={report[ds]['gzip_ratio']:.2f}× "
            f"zstd={report[ds]['zstd_ratio']:.2f}× "
            f"br={report[ds]['brotli_ratio']:.2f}× "
            f"xz={report[ds]['xz_ratio']:.2f}× "
            f"bz2={report[ds]['bz2_ratio']:.2f}× "
            f"lz4={report[ds]['lz4_ratio']:.2f}×"
        )

    OUT.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"\n✅ wrote: {OUT}")

if __name__ == "__main__":
    main()
