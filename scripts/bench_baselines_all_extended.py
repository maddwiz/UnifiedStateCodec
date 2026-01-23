from __future__ import annotations

import gzip
import json
import shutil
import subprocess
import time
from pathlib import Path

RAW_DIR = Path("results/raw_all_200k")
OUT = Path("results/baselines_all_extended.json")


def have(tool: str) -> bool:
    return shutil.which(tool) is not None


def run(cmd: list[str]) -> tuple[int, str, str, float]:
    t0 = time.time()
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return p.returncode, p.stdout, p.stderr, (time.time() - t0)


def run_shell(cmd: str) -> tuple[int, str, str, float]:
    t0 = time.time()
    p = subprocess.run(["bash", "-lc", cmd], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return p.returncode, p.stdout, p.stderr, (time.time() - t0)


def size_of(path: Path) -> int:
    return path.stat().st_size if path.exists() else 0


def ratio(raw_size: int, comp_size: int) -> float:
    if comp_size <= 0:
        return 0.0
    return raw_size / comp_size


def main():
    if not RAW_DIR.exists():
        raise SystemExit("❌ results/raw_all_200k not found")

    logs = sorted(RAW_DIR.glob("*_200k.log"))
    if not logs:
        raise SystemExit("❌ no *_200k.log files found in results/raw_all_200k")

    print("=== tools ===")
    for t in ["zstd", "brotli", "xz", "lz4", "bzip2"]:
        print(f"{t:<7} {'✅' if have(t) else '❌'}")

    report: dict[str, dict] = {}

    for raw in logs:
        ds = raw.name.replace("_200k.log", "")
        raw_bytes = raw.read_bytes()
        raw_size = len(raw_bytes)

        row: dict[str, object] = {
            "raw_path": str(raw),
            "raw_size": raw_size,
        }

        # gzip -9 (python)
        gz_bytes = gzip.compress(raw_bytes, compresslevel=9)
        gz_size = len(gz_bytes)
        row["gzip_size"] = gz_size
        row["gzip_ratio"] = ratio(raw_size, gz_size)

        # zstd -19
        zst_path = RAW_DIR / f"{ds}_200k.log.zst"
        if have("zstd"):
            rc, out, err, dt = run(["zstd", "-19", "-q", "-f", "-o", str(zst_path), str(raw)])
            if rc != 0:
                print(f"❌ zstd failed for {ds}: {err.strip()}")
        row["zstd_size"] = size_of(zst_path)
        row["zstd_ratio"] = ratio(raw_size, size_of(zst_path))

        # brotli -q 11
        br_path = RAW_DIR / f"{ds}_200k.log.br"
        if have("brotli"):
            rc, out, err, dt = run(["brotli", "-q", "11", "-f", "-o", str(br_path), str(raw)])
            if rc != 0:
                print(f"❌ brotli failed for {ds}: {err.strip()}")
        row["brotli_size"] = size_of(br_path)
        row["brotli_ratio"] = ratio(raw_size, size_of(br_path))

        # xz -9  (binary output -> write to file)
        xz_path = RAW_DIR / f"{ds}_200k.log.xz"
        if have("xz"):
            rc, out, err, dt = run_shell(f"xz -9 -f -c '{raw}' > '{xz_path}'")
            if rc != 0:
                print(f"❌ xz failed for {ds}: {err.strip()}")
        row["xz_size"] = size_of(xz_path)
        row["xz_ratio"] = ratio(raw_size, size_of(xz_path))

        # lz4 -9
        lz4_path = RAW_DIR / f"{ds}_200k.log.lz4"
        if have("lz4"):
            rc, out, err, dt = run(["lz4", "-9", "-f", str(raw), str(lz4_path)])
            if rc != 0:
                print(f"❌ lz4 failed for {ds}: {err.strip()}")
        row["lz4_size"] = size_of(lz4_path)
        row["lz4_ratio"] = ratio(raw_size, size_of(lz4_path))

        # bzip2 -9
        bz2_path = RAW_DIR / f"{ds}_200k.log.bz2"
        if have("bzip2"):
            rc, out, err, dt = run_shell(
                f"bzip2 -9 -f -k '{raw}' && mv -f '{raw}.bz2' '{bz2_path}'"
            )
            if rc != 0:
                print(f"❌ bzip2 failed for {ds}: {err.strip()}")
        row["bzip2_size"] = size_of(bz2_path)
        row["bzip2_ratio"] = ratio(raw_size, size_of(bz2_path))

        report[ds] = row

        print(
            f"✅ {ds:<12} "
            f"gzip={row['gzip_ratio']:.2f}× "
            f"zstd={row['zstd_ratio']:.2f}× "
            f"br={row['brotli_ratio']:.2f}× "
            f"xz={row['xz_ratio']:.2f}× "
            f"lz4={row['lz4_ratio']:.2f}× "
            f"bz2={row['bzip2_ratio']:.2f}×"
        )

    OUT.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"\n✅ wrote: {OUT}")


if __name__ == "__main__":
    main()
