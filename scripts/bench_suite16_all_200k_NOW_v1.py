from __future__ import annotations
from pathlib import Path
import subprocess
import time
import gzip
import zstandard as zstd

RAW_DIR = Path("results/raw_real_suite16_200k")
BIN_DIR = Path("results/suite16_200k")
OUT_CSV = BIN_DIR / "bench_table.csv"

def gzip_bytes(data: bytes, level: int = 9) -> bytes:
    return gzip.compress(data, compresslevel=level)

def zstd_bytes(data: bytes, level: int = 19) -> bytes:
    c = zstd.ZstdCompressor(level=level)
    return c.compress(data)

def run_query_hot(bin_path: Path, q: str = "error", limit: int = 10) -> float:
    t0 = time.time()
    subprocess.run([
        "python3", "-m", "usc.cli.app", "query",
        "--mode", "hot",
        "--hot", str(bin_path),
        "--q", q,
        "--limit", str(limit),
    ], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return (time.time() - t0) * 1000.0

def run_query_hotlite(bin_path: Path, q: str = "error", limit: int = 10) -> float:
    t0 = time.time()
    subprocess.run([
        "python3", "-m", "usc.cli.app", "query",
        "--mode", "hot-lite-full",
        "--input", str(bin_path),
        "--q", q,
        "--limit", str(limit),
    ], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return (time.time() - t0) * 1000.0

logs = sorted(RAW_DIR.glob("*_200000.log"))

rows = []
for p in logs:
    ds = p.name.replace("_200000.log", "")
    raw = p.read_bytes()

    gz = gzip_bytes(raw, 9)
    zs = zstd_bytes(raw, 19)

    hot = BIN_DIR / f"{ds}_hot.bin"
    hlf = BIN_DIR / f"{ds}_hotlitefull.bin"

    hot_size = hot.stat().st_size if hot.exists() else -1
    hlf_size = hlf.stat().st_size if hlf.exists() else -1

    q_hot_ms = run_query_hot(hot, "error", 10) if hot.exists() else -1.0
    q_hlf_ms = run_query_hotlite(hlf, "error", 10) if hlf.exists() else -1.0

    rows.append([
        ds,
        len(raw),
        len(gz),
        len(zs),
        hot_size,
        hlf_size,
        round(q_hot_ms, 2),
        round(q_hlf_ms, 2),
    ])

OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
with OUT_CSV.open("w", encoding="utf-8") as f:
    f.write("dataset,raw_bytes,gzip9_bytes,zstd19_bytes,usc_hot_bytes,usc_hotlitefull_bytes,hot_query_ms,hotlitefull_query_ms\n")
    for r in rows:
        f.write(",".join(str(x) for x in r) + "\n")

print("✅ wrote:", OUT_CSV)
print("Top 8 rows:")
for r in rows[:8]:
    print(r)
