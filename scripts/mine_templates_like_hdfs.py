from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, Tuple

from drain3 import TemplateMiner
from drain3.template_miner_config import TemplateMinerConfig


def iter_lines(log_path: Path, max_lines: int):
    with log_path.open("r", encoding="utf-8", errors="ignore") as f:
        for i, line in enumerate(f):
            if max_lines > 0 and i >= max_lines:
                break
            yield line.rstrip("\n")


def main():
    import argparse

    ap = argparse.ArgumentParser(
        description="Mine Drain3 templates and write HDFS-style template CSV: EventId,EventTemplate"
    )
    ap.add_argument("--log", required=True, help="Path to log file")
    ap.add_argument("--out", required=True, help="Output CSV path")
    ap.add_argument("--lines", type=int, default=200000, help="Max lines to read (default 200k)")
    args = ap.parse_args()

    root = Path(__file__).resolve().parents[1]
    log_path = (root / args.log).resolve()
    out_path = (root / args.out).resolve()

    if not log_path.exists():
        raise SystemExit(f"Missing log: {log_path}")

    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Drain3 miner
    cfg = TemplateMinerConfig()
    cfg.profiling_enabled = False
    miner = TemplateMiner(config=cfg)

    # We build our own stable cluster_id -> template map
    cid_to_tpl: Dict[int, str] = {}

    n = 0
    for raw in iter_lines(log_path, args.lines):
        n += 1
        res = miner.add_log_message(raw)

        # Drain3 always returns a cluster_id for a successful parse
        cid = res.get("cluster_id", None)
        if cid is None:
            continue

        try:
            cid_int = int(cid)
        except Exception:
            continue

        # Template string (Drain3 varies by version on key names)
        tpl = res.get("template_mined") or res.get("cluster_template") or ""
        if not tpl:
            # last resort: keep original line as "template"
            tpl = raw

        cid_to_tpl[cid_int] = tpl

    # Sort templates by cluster_id for stable IDs
    items: list[Tuple[int, str]] = sorted(cid_to_tpl.items(), key=lambda x: x[0])

    # Write HDFS-style template CSV
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["EventId", "EventTemplate"])

        for i, (_cid, tpl) in enumerate(items, start=1):
            event_id = f"E{i:06d}"
            w.writerow([event_id, tpl])

    print(f"DONE ✅")
    print(f"log:   {log_path}")
    print(f"lines: {n}")
    print(f"tpls:  {len(items)}")
    print(f"out:   {out_path}")


if __name__ == "__main__":
    main()
