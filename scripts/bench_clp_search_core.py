from __future__ import annotations

import json
import os
import subprocess
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ARCH_DIR = ROOT / "results" / "clp" / "archives"
OUT_JSON = ROOT / "results" / "clp" / "clp_search.json"

IMAGE = "ghcr.io/y-scope/clp/clp-core-x86-ubuntu-jammy:main"
PLATFORM = "linux/amd64"

DATASETS = ["Android", "Apache", "BGL", "HDFS", "Zookeeper"]

# Keep it simple and consistent:
# We'll run a few common queries and measure time.
QUERIES = [
    "ERROR",
    "Exception",
    "WARN",
    "INFO",
]

def run(cmd: list[str]) -> tuple[int, str, str]:
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return p.returncode, p.stdout, p.stderr

def docker_bash(script: str) -> tuple[float, str]:
    t0 = time.time()
    rc, out, err = run([
        "docker", "run", "--rm",
        "--platform", PLATFORM,
        "-u", f"{os.getuid()}:{os.getgid()}",
        "-v", f"{ROOT}/results/clp/archives:/mnt/data",
        IMAGE, "bash", "-lc", script
    ])
    dt = time.time() - t0
    if rc != 0:
        raise RuntimeError(f"docker failed:\nSTDOUT:\n{out}\nSTDERR:\n{err}")
    return dt, out + err

def find_clg() -> str:
    script = r"""
set -e
if command -v clg >/dev/null 2>&1; then echo "CLG=clg"; exit 0; fi
if [ -x ./clg ]; then echo "CLG=./clg"; exit 0; fi
BIN=$(find / -maxdepth 6 -type f -name clg 2>/dev/null | head -n 1 || true)
if [ -n "$BIN" ]; then echo "CLG=$BIN"; exit 0; fi
echo "CLG=clg"
"""
    _, out = docker_bash(script)
    for line in out.splitlines():
        if line.startswith("CLG="):
            return line.split("=", 1)[1].strip()
    return "clg"

def main():
    clg = find_clg()
    results: dict[str, dict] = {}

    for ds in DATASETS:
        arch = ARCH_DIR / ds
        if not arch.exists():
            print(f"⚠️ missing archive for {ds}: {arch}")
            continue

        results[ds] = {}

        for q in QUERIES:
            # clg usage: clg <archive_dir> "<query>"
            script = f"""
set -e
{clg} /mnt/data/{ds} "{q}" >/dev/null
"""
            dt, _ = docker_bash(script)
            results[ds][q] = {"search_s": dt}
            print(f"✅ CLP search {ds:10s} query={q:10s} time={dt:.3f}s")

    OUT_JSON.write_text(json.dumps(results, indent=2), encoding="utf-8")
    print("✅ wrote:", OUT_JSON)

if __name__ == "__main__":
    main()
