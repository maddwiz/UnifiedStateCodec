from __future__ import annotations

import argparse
from pathlib import Path

from usc.mem.hdfs_templates_v0 import HDFSTemplateBank, parse_hdfs_lines_rows
from usc.mem.tpl_pf1_recall_v3_h1m2 import build_tpl_pf3_blob_h1m2 as build_pf3_h1m2
from usc.mem.tpl_pf3_decode_v1_h1m2 import decode_pf3_h1m2_to_lines


def read_first_n(path: Path, n: int) -> list[str]:
    out = []
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for i, ln in enumerate(f):
            if i >= n:
                break
            out.append(ln.rstrip("\n"))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--log", required=True)
    ap.add_argument("--tpl", required=True)
    ap.add_argument("--lines", type=int, default=200000)
    ap.add_argument("--packet-events", type=int, default=25)
    ap.add_argument("--zstd", type=int, default=10)
    ap.add_argument("--out", default="results/__pf3_roundtrip.bin")
    args = ap.parse_args()

    log_path = Path(args.log)
    tpl_path = Path(args.tpl)
    out_path = Path(args.out)

    raw_lines = read_first_n(log_path, args.lines)

    bank = HDFSTemplateBank.from_csv(tpl_path)
    rows, unknown = parse_hdfs_lines_rows(raw_lines, bank)
    tpl_text = tpl_path.read_text(encoding="utf-8", errors="ignore")

    pf3_blob, meta = build_pf3_h1m2(rows, unknown, tpl_text, packet_events=args.packet_events, zstd_level=args.zstd)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(pf3_blob)

    print("PF3 bytes:", len(pf3_blob))
    decoded = decode_pf3_h1m2_to_lines(pf3_blob)

    if len(decoded) != len(raw_lines):
        print("❌ LENGTH MISMATCH:", len(raw_lines), "vs", len(decoded))
        raise SystemExit(1)

    for i, (a, b) in enumerate(zip(raw_lines, decoded)):
        if a != b:
            print("❌ MISMATCH at line", i)
            print("ORIG:", a)
            print("DECO:", b)
            raise SystemExit(2)

    print("✅ PF3 ROUNDTRIP OK:", len(raw_lines), "lines matched exactly")


if __name__ == "__main__":
    main()
