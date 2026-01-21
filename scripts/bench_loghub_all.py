from __future__ import annotations

import json
import time
import subprocess
import sys
import os
from pathlib import Path
from typing import Dict, List, Tuple

import gzip

try:
    import zstandard as zstd
except Exception:
    zstd = None

try:
    import brotli
except Exception:
    brotli = None


ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data" / "loghub"
PRE = DATA / "preprocessed"
RESULTS = ROOT / "results"

PY = sys.executable

USC_MODES = ["stream", "hot-lite", "hot", "cold"]

# ✅ HOT-LITE/HOT currently act like index/skeleton on some datasets
# until PF1 params are implemented for them.
INDEX_ONLY_MODES = {"hot-lite", "hot"}


def _read_lines_bytes(log_path: Path, max_lines: int) -> bytes:
    out_lines: List[str] = []
    with log_path.open("r", encoding="utf-8", errors="ignore") as f:
        for i, line in enumerate(f):
            if max_lines > 0 and i >= max_lines:
                break
            out_lines.append(line.rstrip("\n"))
    return ("\n".join(out_lines) + "\n").encode("utf-8", errors="replace")


def _pretty(n: int) -> str:
    if n < 1024:
        return f"{n} B"
    if n < 1024 * 1024:
        return f"{n / 1024:.2f} KB"
    return f"{n / (1024 * 1024):.2f} MB"


def _ratio(raw_n: int, comp_n: int) -> float:
    if comp_n <= 0:
        return 0.0
    return raw_n / comp_n


def _bench_gzip(raw: bytes) -> Tuple[int, float]:
    t0 = time.perf_counter()
    comp = gzip.compress(raw, compresslevel=9)
    dt = (time.perf_counter() - t0) * 1000
    return len(comp), dt


def _bench_zstd(raw: bytes, level: int = 19) -> Tuple[int, float]:
    if zstd is None:
        return 0, 0.0
    t0 = time.perf_counter()
    comp = zstd.ZstdCompressor(level=level).compress(raw)
    dt = (time.perf_counter() - t0) * 1000
    return len(comp), dt


def _bench_brotli(raw: bytes, quality: int = 11) -> Tuple[int, float]:
    if brotli is None:
        return 0, 0.0
    t0 = time.perf_counter()
    comp = brotli.compress(raw, quality=quality)
    dt = (time.perf_counter() - t0) * 1000
    return len(comp), dt


def _ensure_templates(log_path: Path, lines: int) -> Path:
    PRE.mkdir(parents=True, exist_ok=True)
    out_csv = PRE / f"{log_path.name}_templates.csv"
    if out_csv.exists():
        return out_csv

    cmd = [
        PY,
        "scripts/mine_templates_like_hdfs.py",
        "--log", str(log_path.relative_to(ROOT)),
        "--out", str(out_csv.relative_to(ROOT)),
        "--lines", str(lines),
    ]

    env = os.environ.copy()
    env["PYTHONPATH"] = "src"

    print(f"[MINER] {log_path.name} -> {out_csv.name}")
    subprocess.check_call(cmd, cwd=str(ROOT), env=env)
    return out_csv


def _run_usc_mode(log_path: Path, mode: str, lines: int, tpl_path: Path | None) -> Tuple[int, float]:
    out_bin = RESULTS / f"__tmp_{log_path.stem}_{mode}.bin"
    out_bin.parent.mkdir(parents=True, exist_ok=True)

    cmd = [
        PY,
        "-m",
        "usc",
        "encode",
        "--mode", mode,
        "--log", str(log_path),
        "--lines", str(lines),
        "--out", str(out_bin),
    ]

    if mode != "stream" and tpl_path is not None:
        cmd += ["--tpl", str(tpl_path)]

    env = os.environ.copy()
    env["PYTHONPATH"] = "src"

    t0 = time.perf_counter()
    subprocess.check_call(cmd, cwd=str(ROOT), env=env)
    dt = (time.perf_counter() - t0) * 1000

    return out_bin.stat().st_size, dt


def main():
    import argparse

    ap = argparse.ArgumentParser(description="Bench ALL LogHub logs vs baselines + USC modes")
    ap.add_argument("--lines", type=int, default=int(os.environ.get("USC_SUITE_LINES", "200000")))
    args = ap.parse_args()

    lines = int(args.lines)

    logs = sorted([p for p in DATA.glob("*.log") if p.is_file()])
    if not logs:
        raise SystemExit(f"No *.log files found in {DATA}")

    results: Dict[str, Dict[str, Dict[str, float]]] = {}

    print(f"\nFOUND {len(logs)} log files in {DATA}")
    print(f"LINES TARGET: {lines}\n")

    for log_path in logs:
        print(f"\n=== DATASET: {log_path.stem} ===")
        raw = _read_lines_bytes(log_path, lines)
        raw_n = len(raw)
        print(f"raw={_pretty(raw_n)}")

        tpl_csv = _ensure_templates(log_path, lines)

        results[log_path.stem] = {}

        gz_n, gz_ms = _bench_gzip(raw)
        zs_n, zs_ms = _bench_zstd(raw, level=19)
        br_n, br_ms = _bench_brotli(raw, quality=11)

        results[log_path.stem]["gzip"] = {"bytes": gz_n, "ratio": _ratio(raw_n, gz_n), "ms": gz_ms}
        if zs_n > 0:
            results[log_path.stem]["zstd-19"] = {"bytes": zs_n, "ratio": _ratio(raw_n, zs_n), "ms": zs_ms}
        if br_n > 0:
            results[log_path.stem]["brotli-11"] = {"bytes": br_n, "ratio": _ratio(raw_n, br_n), "ms": br_ms}

        print(f"gzip         {_pretty(gz_n):>10}  ratio {_ratio(raw_n,gz_n):7.2f}x  {gz_ms:8.1f} ms")
        if zs_n > 0:
            print(f"zstd-19      {_pretty(zs_n):>10}  ratio {_ratio(raw_n,zs_n):7.2f}x  {zs_ms:8.1f} ms")
        if br_n > 0:
            print(f"brotli-11    {_pretty(br_n):>10}  ratio {_ratio(raw_n,br_n):7.2f}x  {br_ms:8.1f} ms")

        for mode in USC_MODES:
            try:
                n, ms = _run_usc_mode(log_path, mode, lines, tpl_csv)

                if mode in INDEX_ONLY_MODES:
                    # ✅ we still log bytes+ms, but don't pretend it's lossless compression
                    results[log_path.stem][f"USC-{mode}"] = {
                        "bytes": n,
                        "ratio": _ratio(raw_n, n),
                        "ms": ms,
                        "note": "INDEX_ONLY_UNTIL_PF1_PARAMS",
                    }
                    print(f"USC-{mode:<8} {_pretty(n):>10}  (INDEX-ONLY)  {ms:8.1f} ms")
                else:
                    results[log_path.stem][f"USC-{mode}"] = {"bytes": n, "ratio": _ratio(raw_n, n), "ms": ms}
                    print(f"USC-{mode:<8} {_pretty(n):>10}  ratio {_ratio(raw_n,n):7.2f}x  {ms:8.1f} ms")

            except subprocess.CalledProcessError:
                results[log_path.stem][f"USC-{mode}"] = {"bytes": 0, "ratio": 0.0, "ms": 0.0, "error": 1}
                print(f"USC-{mode:<8} ERROR")

    out_json = RESULTS / "bench_loghub_all.json"
    out_json.write_text(json.dumps(results, indent=2), encoding="utf-8")
    print(f"\nWROTE: {out_json}")


if __name__ == "__main__":
    main()
