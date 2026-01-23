from __future__ import annotations

import json
import subprocess
import time
from pathlib import Path

OUT = Path("results/usc_query_speed_hot_lite_full.json")

DATASETS = [
    "Android_v2",
    "Apache",
    "BGL",
    "HDFS_v2",
    "HPC",
    "Hadoop",
    "HealthApp",
    "Linux",
    "Mac",
    "OpenStack",
    "Proxifier",
    "SSH",
    "Spark",
    "Thunderbird",
    "Windows",
    "Zookeeper",
]

QUERIES = ["the", "Starting", "ERROR", "WARN", "INFO", "Exception"]

def run(cmd: list[str]) -> tuple[int, str, float]:
    t0 = time.time()
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    return p.returncode, p.stdout, (time.time() - t0)

def main():
    rows = []

    for ds in DATASETS:
        blob = Path(f"results/__tmp_{ds}_hot-lite-full.bin")
        if not blob.exists():
            print(f"⚠️ missing blob: {blob}")
            continue

        for q in QUERIES:
            tmp_out = Path("/tmp/usc_q_decode_tmp.log")

            # decode
            rc1, out1, dt1 = run([
                "python3", "-m", "usc.cli.app", "decode",
                "--mode", "hot-lite-full",
                "--input", str(blob),
                "--out", str(tmp_out),
            ])

            # grep (first 10 hits)
            rc2, out2, dt2 = run([
                "bash", "-lc", f"grep -i -m 10 {q!r} {tmp_out} || true"
            ])

            hits = 0
            if out2.strip():
                hits = len(out2.strip().splitlines())

            row = {
                "dataset": ds,
                "q": q,
                "decode_wall_s": dt1,
                "grep_wall_s": dt2,
                "hits_returned": hits,
                "decode_ok": (rc1 == 0),
            }
            rows.append(row)

            print(f"✅ {ds:12s} q={q:10s} hits={hits:<3d} decode={dt1:.3f}s grep={dt2:.3f}s")

    OUT.write_text(json.dumps(rows, indent=2), encoding="utf-8")
    print(f"\n✅ wrote: {OUT}")

if __name__ == "__main__":
    main()
