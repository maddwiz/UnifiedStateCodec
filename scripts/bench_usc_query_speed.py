from __future__ import annotations

import json
import re
import subprocess
import time
from pathlib import Path

RAW_DIR = Path("results/raw_real_suite16_200k")
OUT = Path("results/usc_query_speed.json")

QUERIES = ["ERROR", "Exception", "WARN", "INFO"]

HITS_RE = re.compile(r"hits:\s*(\d+)\s+time=([\d\.]+)\s*ms", re.IGNORECASE)
MODE_RE = re.compile(r"mode:\s*([A-Z0-9_]+)", re.IGNORECASE)


def run(cmd: list[str]) -> tuple[int, str, str, float]:
    t0 = time.time()
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return p.returncode, p.stdout, p.stderr, (time.time() - t0)


def list_datasets() -> list[str]:
    logs = sorted(RAW_DIR.glob("*_200000.log"))
    return [p.name.replace("_200000.log", "") for p in logs]


def find_hot_blob(ds: str) -> Path | None:
    p = Path(f"results/__tmp_{ds}_hot.bin")
    if p.exists():
        return p
    cands = list(Path("results").glob(f"__tmp_{ds}_hot*.bin"))
    cands = [x for x in cands if x.exists()]
    if not cands:
        return None
    cands.sort(key=lambda x: x.stat().st_size, reverse=True)
    return cands[0]


def parse_hits_mode(stdout: str) -> tuple[int | None, str | None, float | None]:
    hits = None
    mode = None
    ms = None

    m = MODE_RE.search(stdout)
    if m:
        mode = m.group(1)

    m = HITS_RE.search(stdout)
    if m:
        hits = int(m.group(1))
        ms = float(m.group(2))

    return hits, mode, ms


def main():
    if not RAW_DIR.exists():
        raise SystemExit("❌ results/raw_real_suite16_200k not found")

    datasets = list_datasets()
    if not datasets:
        raise SystemExit("❌ no *_200000.log found")

    report: dict[str, dict] = {}

    for ds in datasets:
        hot_blob = find_hot_blob(ds)
        row: dict[str, object] = {
            "dataset": ds,
            "hot_blob": str(hot_blob) if hot_blob else None,
            "exists": bool(hot_blob and hot_blob.exists()),
        }

        if not hot_blob:
            report[ds] = row
            print(f"⚠️ USC query {ds:<12} missing HOT blob")
            continue

        for q in QUERIES:
            cmd = [
                "python3",
                "-m",
                "usc.cli.app",
                "query",
                "--hot",
                str(hot_blob),
                "--q",
                q,
                "--limit",
                "50",
            ]

            rc, out, err, dt = run(cmd)

            # ✅ treat rc=0 (hits) and rc=1 (0 hits) as "success"
            ok = (rc in (0, 1))

            hits, mode, ms = parse_hits_mode(out)

            row[q] = {
                "ok": ok,
                "rc": rc,
                "wall_s": dt,
                "mode": mode,
                "hits": hits,
                "query_ms_reported": ms,
                "stderr_tail": "\n".join(err.strip().splitlines()[-10:]) if err else "",
                "stdout_tail": "\n".join(out.strip().splitlines()[-12:]) if out else "",
            }

            if ok:
                print(f"✅ USC query {ds:<12} q={q:<10} rc={rc} hits={hits} wall={dt:.4f}s")
            else:
                print(f"❌ USC query {ds:<12} q={q:<10} rc={rc} wall={dt:.4f}s")

        report[ds] = row

    OUT.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"\n✅ wrote: {OUT}")


if __name__ == "__main__":
    main()
