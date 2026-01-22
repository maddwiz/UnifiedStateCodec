from __future__ import annotations

import json
import subprocess
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "results" / "usc_query_speed.json"

DATASETS = ["Android", "Apache", "BGL", "HDFS", "Zookeeper"]
QUERIES = ["ERROR", "Exception", "WARN", "INFO"]


def run(cmd: list[str]) -> tuple[int, str, str, float]:
    t0 = time.time()
    p = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    dt = time.time() - t0
    return p.returncode, p.stdout, p.stderr, dt


def main():
    results: dict[str, dict] = {}

    for ds in DATASETS:
        pe_path = ROOT / "results" / f"__tmp_{ds}_hot-lite-full.bin"
        results[ds] = {"packet_events": str(pe_path), "exists": pe_path.exists()}

        if not pe_path.exists():
            print(f"❌ missing packet_events file: {pe_path}")
            continue

        for q in QUERIES:
            cmd = [
                "python3", "-m", "usc", "query",
                "--packet_events", str(pe_path),
                "--q", q,
                "--limit", "0",
            ]

            rc, out, err, dt = run(cmd)

            results[ds][q] = {
                "ok": (rc == 0),
                "query_s": dt,
                "stdout_tail": out.strip().splitlines()[-1] if out.strip() else "",
                "stderr_tail": err.strip().splitlines()[-1] if err.strip() else "",
            }

            if rc == 0:
                print(f"✅ USC query {ds:<10} q={q:<10} time={dt:.4f}s")
            else:
                print(f"❌ USC query failed {ds} q={q}")
                if err.strip():
                    print(err.strip())

    OUT.write_text(json.dumps(results, indent=2), encoding="utf-8")
    print(f"✅ wrote: {OUT}")


if __name__ == "__main__":
    main()
