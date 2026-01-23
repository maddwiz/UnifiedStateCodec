from __future__ import annotations

import json
import os
import subprocess
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LOGS_DIR = ROOT / "results" / "clp" / "logs"
ARCH_DIR = ROOT / "results" / "clp" / "archives"
OUT_JSON = ROOT / "results" / "clp" / "clp_bench.json"

IMAGE = "ghcr.io/y-scope/clp/clp-core-x86-ubuntu-jammy:main"

DATASETS = ["Android", "Apache", "BGL", "HDFS", "Zookeeper"]

def run(cmd: list[str]) -> tuple[int, str, str]:
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return p.returncode, p.stdout, p.stderr

def du_bytes(path: Path) -> int:
    # bytes on linux: du -sb
    rc, out, err = run(["du", "-sb", str(path)])
    if rc != 0:
        raise RuntimeError(f"du failed: {err}")
    return int(out.split()[0])

def docker_bash(script: str) -> tuple[float, str]:
    """
    Run bash script inside CLP container with mounts:
      - logs -> /mnt/logs
      - archives/results -> /mnt/data
    """
    t0 = time.time()
    rc, out, err = run([
        "docker", "run", "--platform=linux/amd64", "--platform=linux/amd64", "--rm", "-u", f"{os.getuid()}:{os.getgid()}",
        "-v", f"{ROOT}/results/clp/logs:/mnt/logs",
        "-v", f"{ROOT}/results/clp/archives:/mnt/data",
        IMAGE, "bash", "-lc", script
    ])
    dt = time.time() - t0
    if rc != 0:
        raise RuntimeError(f"docker failed:\nSTDOUT:\n{out}\nSTDERR:\n{err}")
    return dt, out + err

def main():
    ARCH_DIR.mkdir(parents=True, exist_ok=True)

    results: dict[str, dict] = {}

    # locate clp binary inside container
    # docs show usage as ./clp, but we’ll find it robustly.
    find_bin = r"""
set -e
if command -v clp >/dev/null 2>&1; then echo "CLP_BIN=clp"; exit 0; fi
if [ -x ./clp ]; then echo "CLP_BIN=./clp"; exit 0; fi
BIN=$(find / -maxdepth 4 -type f -name clp 2>/dev/null | head -n 1 || true)
if [ -n "$BIN" ]; then echo "CLP_BIN=$BIN"; exit 0; fi
echo "CLP_BIN=./clp"
"""
    _, out = docker_bash(find_bin)
    clp_bin = None
    for line in out.splitlines():
        if line.startswith("CLP_BIN="):
            clp_bin = line.split("=", 1)[1].strip()
            break
    if not clp_bin:
        clp_bin = "./clp"

    for ds in DATASETS:
        log = LOGS_DIR / f"{ds}_200k.log"
        arch = ARCH_DIR / ds
        if arch.exists():
            # wipe old archives to keep sizes consistent
            for p in arch.rglob("*"):
                if p.is_file():
                    p.unlink()
            for p in sorted([p for p in arch.rglob("*") if p.is_dir()], reverse=True):
                try:
                    p.rmdir()
                except OSError:
                    pass
        arch.mkdir(parents=True, exist_ok=True)

        # CLP compression for unstructured logs:
        #   clp c <archives-dir> <input-path>
        # docs: ./clp c /mnt/data/archives1 /mnt/logs/log1.log  [oai_citation:3‡YScope Docs](https://docs.yscope.com/clp/main/user-docs/core-unstructured/clp.html)
        script = f"""
set -e
{clp_bin} c /mnt/data/{ds} /mnt/logs/{ds}_200k.log
"""
        dt, _ = docker_bash(script)

        size_b = du_bytes(arch)
        raw_b = log.stat().st_size
        ratio = (raw_b / size_b) if size_b else 0.0

        results[ds] = {
            "raw_bytes": raw_b,
            "clp_bytes": size_b,
            "ratio": ratio,
            "encode_s": dt,
        }
        print(f"✅ CLP {ds}: {size_b/1024:.2f} KB  ratio {ratio:.2f}×  time {dt:.2f}s")

    OUT_JSON.write_text(json.dumps(results, indent=2), encoding="utf-8")
    print("✅ wrote:", OUT_JSON)

if __name__ == "__main__":
    main()
