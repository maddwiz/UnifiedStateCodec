from __future__ import annotations

import argparse
import json
import subprocess
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "results"

DATASETS = ["Android", "Apache", "BGL", "HDFS", "Zookeeper"]

def sh(cmd: list[str]) -> tuple[int, str, str, float]:
    t0 = time.time()
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    dt = time.time() - t0
    return p.returncode, p.stdout, p.stderr, dt

def must_exist(p: Path) -> bool:
    return p.exists() and p.stat().st_size > 0

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lines", type=int, default=200_000)
    ap.add_argument("--out", type=str, default=str(RESULTS / "bench_all.json"))
    args = ap.parse_args()

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    report: dict[str, object] = {
        "lines": int(args.lines),
        "root": str(ROOT),
        "results_dir": str(RESULTS),
        "datasets": {},
        "steps": {},
    }

    # ---------------------------------------------------------
    # Step A: Verify required 200k artifacts exist
    # ---------------------------------------------------------
    print("\n=== STEP A: verify required artifacts ===")
    missing = []
    for ds in DATASETS:
        pe = RESULTS / f"__tmp_{ds}_hot-lite-full.bin"
        ok = must_exist(pe)
        report["datasets"].setdefault(ds, {})
        report["datasets"][ds]["packet_events_file"] = str(pe)
        report["datasets"][ds]["packet_events_exists"] = ok
        if not ok:
            missing.append(str(pe))
            print(f"❌ missing: {pe}")
        else:
            print(f"✅ found:   {pe.name} ({pe.stat().st_size} bytes)")

    if missing:
        print("\n❌ Missing required artifacts. Stop here.")
        print("➡️ Generate the __tmp_*_hot-lite-full.bin files first, then rerun bench_all.py.")
        report["steps"]["A_verify"] = {"ok": False, "missing": missing}
        out_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(f"\n✅ wrote: {out_path}")
        return
    else:
        report["steps"]["A_verify"] = {"ok": True}

    # ---------------------------------------------------------
    # Step B: USC query-speed (packet_events) — THE REAL SEARCH BASELINE
    # ---------------------------------------------------------
    print("\n=== STEP B: USC query speed (packet_events) ===")
    rc, out, err, dt = sh(["python3", "scripts/bench_usc_query_speed.py"])
    report["steps"]["B_query_speed"] = {
        "ok": (rc == 0),
        "wall_s": dt,
        "stderr_tail": err.strip().splitlines()[-1] if err.strip() else "",
    }
    if rc == 0:
        print("✅ query-speed bench OK")
    else:
        print("❌ query-speed bench FAILED")
        if err.strip():
            print(err.strip())

    # ---------------------------------------------------------
    # Step C: Competitor report + README patch
    # ---------------------------------------------------------
    print("\n=== STEP C: competitor report + README patch ===")
    rc1, out1, err1, dt1 = sh(["python3", "scripts/make_competitor_report_md.py"])
    rc2, out2, err2, dt2 = sh(["python3", "scripts/patch_readme_competitors.py"])
    report["steps"]["C_competitor_report"] = {
        "ok": (rc1 == 0 and rc2 == 0),
        "make_report_s": dt1,
        "patch_readme_s": dt2,
        "make_report_err": err1.strip().splitlines()[-1] if err1.strip() else "",
        "patch_readme_err": err2.strip().splitlines()[-1] if err2.strip() else "",
    }

    if rc1 == 0 and rc2 == 0:
        print("✅ competitor report + README patch OK")
        print("✅ wrote: results/competitor_report.md")
    else:
        print("❌ competitor report step FAILED")
        if err1.strip():
            print(err1.strip())
        if err2.strip():
            print(err2.strip())

    # ---------------------------------------------------------
    # Write final summary JSON
    # ---------------------------------------------------------
    out_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"\n✅ wrote: {out_path}")

if __name__ == "__main__":
    main()
