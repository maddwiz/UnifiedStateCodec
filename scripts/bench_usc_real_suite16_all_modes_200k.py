from __future__ import annotations

import json
import subprocess
import time
from pathlib import Path

RAW_DIR = Path("results/raw_real_suite16_200k")
OUT_JSON = Path("results/bench_usc_real_suite16_all_modes_200k.json")

MODES = [
    ("stream",      ["--chunk_lines", "25"]),
    ("hot",         []),
    ("hot-lite",    []),
    ("hot-lite-full",[]),
    ("cold",        []),
]

# You can tune these defaults later, but keep stable for benchmarking
DEFAULT_PACKET_EVENTS = "512"
DEFAULT_ZSTD = "19"
DEFAULT_LINES = "200000"

def run(cmd: list[str]) -> tuple[int, str, float]:
    t0 = time.time()
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    return p.returncode, p.stdout, (time.time() - t0)

def ratio(raw: int, comp: int) -> float:
    return (raw / comp) if comp > 0 else 0.0

def main():
    if not RAW_DIR.exists():
        raise SystemExit("❌ missing results/raw_real_suite16_200k")

    logs = sorted(RAW_DIR.glob("*_200000.log"))
    if not logs:
        raise SystemExit("❌ no *_200000.log found")

    results: dict[str, dict] = {}

    print("=== USC REAL suite16 @200k — all modes ===")
    print(f"datasets={len(logs)} lines={DEFAULT_LINES} packet_events={DEFAULT_PACKET_EVENTS} zstd={DEFAULT_ZSTD}")

    for raw in logs:
        ds = raw.name.replace("_200000.log", "")
        raw_size = raw.stat().st_size

        print(f"\n--- {ds} --- raw={raw_size/1024/1024:.2f} MB")

        ds_row: dict[str, dict] = {"raw_size": raw_size}

        for mode, extra in MODES:
            out_bin = Path(f"results/__tmp_{ds}_{mode}.bin")

            cmd = [
                "python3", "-m", "usc.cli.app", "encode",
                "--mode", mode,
                "--log", str(raw),
                "--lines", DEFAULT_LINES,
                "--out", str(out_bin),
                "--packet_events", DEFAULT_PACKET_EVENTS,
                "--zstd", DEFAULT_ZSTD,
            ] + extra

            rc, text, dt = run(cmd)

            comp_size = out_bin.stat().st_size if out_bin.exists() else 0
            ds_row[mode] = {
                "rc": rc,
                "seconds": dt,
                "out": str(out_bin),
                "comp_size": comp_size,
                "ratio": ratio(raw_size, comp_size),
                "stdout_tail": "\n".join(text.strip().splitlines()[-8:]),
            }

            if rc == 0:
                print(f"✅ {mode:<12} {ds_row[mode]['ratio']:.2f}×  out={comp_size/1024:.1f} KB  time={dt:.3f}s")
            else:
                print(f"❌ {mode:<12} rc={rc} time={dt:.3f}s (see json tail)")

        results[ds] = ds_row

    OUT_JSON.write_text(json.dumps(results, indent=2), encoding="utf-8")
    print(f"\n✅ wrote: {OUT_JSON}")

if __name__ == "__main__":
    main()
