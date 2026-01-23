from __future__ import annotations

import subprocess
from pathlib import Path

SCRIPTS = Path("scripts")

def run(cmd: list[str]) -> int:
    print("\n>>>", " ".join(cmd))
    p = subprocess.run(cmd)
    return p.returncode

def exists(name: str) -> bool:
    return (SCRIPTS / name).exists()

def main():
    # Candidate scripts (we try what exists in your repo)
    steps = [
        # USC encode ratios (queryable + cold)
        "bench_usc_all.py",
        "bench_usc_200k.py",
        "bench41.py",
        "bench_usc.py",

        # USC query speed bench
        "bench_usc_query_speed.py",
        "bench_query_speed.py",
        "bench_search_speed.py",

        # Make competitor report / scoreboard
        "make_competitor_report_md.py",
        "make_scoreboard_full_200k.py",
    ]

    found = [s for s in steps if exists(s)]
    print("✅ scripts found:")
    for s in found:
        print("  -", s)

    if not found:
        raise SystemExit("❌ Could not find any bench scripts in ./scripts. Run: ls scripts")

    # Run in order: try each known one
    for s in found:
        rc = run(["python3", str(SCRIPTS / s)])
        if rc != 0:
            print(f"⚠️ {s} exited with rc={rc} (continuing)")

    # Show the main outputs if they exist
    for out in [
        Path("results/competitor_report.md"),
        Path("results/scoreboard_full_200k.md"),
    ]:
        if out.exists():
            print("\n===== OUTPUT:", out, "=====")
            print(out.read_text(encoding="utf-8")[:4000])

if __name__ == "__main__":
    main()
