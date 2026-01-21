from __future__ import annotations

import csv
import json
import os
import shutil
import subprocess
import time
from dataclasses import dataclass, asdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "data" / "real_datasets" / "manifest.csv"
OUTDIR = ROOT / "results"
OUTDIR.mkdir(exist_ok=True)


@dataclass
class RunResult:
    dataset: str
    method: str
    raw_bytes: int
    out_bytes: int
    ratio: float
    ms: float
    note: str = ""


def _read_first_n_lines(path: Path, n: int) -> list[str]:
    lines: list[str] = []
    with path.open("r", errors="ignore") as f:
        for _ in range(n):
            ln = f.readline()
            if not ln:
                break
            lines.append(ln.rstrip("\n"))
    return lines


def _bytes_of_text(lines: list[str]) -> bytes:
    return ("\n".join(lines) + "\n").encode("utf-8", errors="ignore")


def _run_cmd_capture(cmd: list[str], stdin_bytes: bytes | None = None) -> tuple[int, bytes, bytes, float]:
    t0 = time.time()
    p = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE if stdin_bytes is not None else None,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    out, err = p.communicate(input=stdin_bytes)
    ms = (time.time() - t0) * 1000.0
    return p.returncode, out, err, ms


def _tool_exists(name: str) -> bool:
    return shutil.which(name) is not None


def _bench_gzip(raw: bytes) -> tuple[int, float, str]:
    if not _tool_exists("gzip"):
        return 0, 0.0, "gzip missing"
    code, out, err, ms = _run_cmd_capture(["gzip", "-c"], stdin_bytes=raw)
    if code != 0:
        return 0, ms, f"gzip failed: {err.decode(errors='ignore')}"
    return len(out), ms, ""


def _bench_zstd(raw: bytes, level: int = 19) -> tuple[int, float, str]:
    if not _tool_exists("zstd"):
        return 0, 0.0, "zstd missing"
    code, out, err, ms = _run_cmd_capture(["zstd", f"-{level}", "-q", "-c"], stdin_bytes=raw)
    if code != 0:
        return 0, ms, f"zstd failed: {err.decode(errors='ignore')}"
    return len(out), ms, ""


def _bench_brotli(raw: bytes, level: int = 11) -> tuple[int, float, str]:
    if not _tool_exists("brotli"):
        return 0, 0.0, "brotli missing (skipped)"
    code, out, err, ms = _run_cmd_capture(["brotli", "-q", str(level), "-c"], stdin_bytes=raw)
    if code != 0:
        return 0, ms, f"brotli failed: {err.decode(errors='ignore')}"
    return len(out), ms, ""


def _bench_usc_stream(lines: list[str], chunk_lines: int = 250) -> tuple[int, float, str]:
    tmp_out = OUTDIR / "__tmp_stream.uscstream"
    cmd = [
        "python",
        "-m",
        "usc",
        "encode",
        "--mode",
        "stream",
        "--out",
        str(tmp_out),
        "--lines",
        str(len(lines)),
        "--chunk_lines",
        str(chunk_lines),
    ]
    t0 = time.time()
    code = subprocess.call(cmd, cwd=str(ROOT), env={**os.environ, "PYTHONPATH": str(ROOT / "src")})
    ms = (time.time() - t0) * 1000.0
    if code != 0 or not tmp_out.exists():
        return 0, ms, "usc stream failed"
    out_bytes = tmp_out.stat().st_size
    tmp_out.unlink(missing_ok=True)
    return out_bytes, ms, ""


def main():
    if not MANIFEST.exists():
        raise SystemExit(f"Manifest missing: {MANIFEST}")

    # Safe defaults
    LINES = int(os.environ.get("USC_SUITE_LINES", "20000"))
    CHUNK_LINES = int(os.environ.get("USC_SUITE_CHUNK_LINES", "250"))

    results: list[RunResult] = []

    with MANIFEST.open("r", newline="") as f:
        rows = list(csv.DictReader(f))

    for row in rows:
        name = row["name"].strip()
        path = (ROOT / row["path"].strip()).resolve()

        if not path.exists():
            print(f"[SKIP] {name} missing file: {path}")
            continue

        print(f"\n=== DATASET: {name} ===")
        lines = _read_first_n_lines(path, LINES)
        raw = _bytes_of_text(lines)
        raw_bytes = len(raw)
        print(f"lines={len(lines)} raw={raw_bytes/1024/1024:.2f} MB")

        def add(method: str, out_bytes: int, ms: float, note: str):
            results.append(RunResult(
                dataset=name,
                method=method,
                raw_bytes=raw_bytes,
                out_bytes=out_bytes,
                ratio=raw_bytes / max(out_bytes, 1),
                ms=ms,
                note=note,
            ))
            if out_bytes > 0:
                print(f"{method:10} {out_bytes/1024:.2f} KB  ratio {raw_bytes/max(out_bytes,1):.2f}x  {ms:.1f} ms {note}")
            else:
                print(f"{method:10} (skipped) {note}")

        out_bytes, ms, note = _bench_gzip(raw)
        add("gzip", out_bytes, ms, note)

        out_bytes, ms, note = _bench_zstd(raw, 19)
        add("zstd-19", out_bytes, ms, note)

        out_bytes, ms, note = _bench_brotli(raw, 11)
        add("brotli-11", out_bytes, ms, note)

        out_bytes, ms, note = _bench_usc_stream(lines, CHUNK_LINES)
        add("USC-STREAM", out_bytes, ms, note)

    out_json = OUTDIR / "bench_real_suite.json"
    out_json.write_text(json.dumps([asdict(x) for x in results], indent=2))
    print(f"\nWROTE: {out_json}")


if __name__ == "__main__":
    main()
