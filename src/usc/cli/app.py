import argparse
import gzip
import json
import os
import struct
import time
from typing import Dict, List, Tuple

try:
    import zstandard as zstd
except Exception:
    zstd = None

from usc.mem.hdfs_templates_v0 import HDFSTemplateBank, parse_hdfs_lines
from usc.mem.tpl_pf1_recall_v1 import build_tpl_pf1_blob as build_pf1
from usc.mem.tpl_pfq1_query_v1 import build_pfq1_blob as build_pfq1
from usc.mem.tpl_fast_query_v1 import query_fast_pf1
from usc.mem.tpl_query_router_v1 import query_router_v1
from usc.api.hdfs_template_codec_v1m_bundle import bundle_encode_and_compress_v1m


MAGIC_HOT = b"USCH"   # PF1 + optional PFQ1 container
MAGIC_COLD = b"USCC"  # TPLv1M bundle container
VERSION = 1


def _read_first_n_lines(path: str, n: int) -> List[str]:
    out = []
    with open(path, "r", errors="replace") as f:
        for _ in range(n):
            ln = f.readline()
            if not ln:
                break
            out.append(ln.rstrip("\n"))
    return out


def _pretty(n: int) -> str:
    if n >= 1_000_000:
        return f"{n/1_000_000:.2f} MB"
    if n >= 1_000:
        return f"{n/1_000:.2f} KB"
    return f"{n} B"


def _ratio(raw: int, comp: int) -> float:
    return raw / max(1, comp)


def _u32(x: int) -> bytes:
    return struct.pack("<I", int(x))


def _u32_read(b: bytes, off: int) -> Tuple[int, int]:
    if off + 4 > len(b):
        raise ValueError("u32 read overflow")
    return struct.unpack("<I", b[off:off + 4])[0], off + 4


# ==========================
# HOT container (PF1 + PFQ1)
# ==========================

def hot_pack(pf1_blob: bytes, pfq1_blob: bytes) -> bytes:
    """
    USCH format:
      MAGIC (4)
      VERSION (u32)
      pf1_len (u32)
      pfq1_len (u32)
      pf1_blob
      pfq1_blob (optional; may be empty)
    """
    return (
        MAGIC_HOT
        + _u32(VERSION)
        + _u32(len(pf1_blob))
        + _u32(len(pfq1_blob))
        + pf1_blob
        + pfq1_blob
    )


def hot_unpack(blob: bytes) -> Tuple[bytes, bytes]:
    if len(blob) < 16:
        raise ValueError("bad USCH blob (too small)")
    if blob[:4] != MAGIC_HOT:
        raise ValueError("not a USCH blob")

    off = 4
    ver, off = _u32_read(blob, off)
    if ver != VERSION:
        raise ValueError(f"unsupported USCH version: {ver}")

    pf1_len, off = _u32_read(blob, off)
    pfq1_len, off = _u32_read(blob, off)

    pf1 = blob[off:off + pf1_len]
    off += pf1_len
    pfq1 = blob[off:off + pfq1_len] if pfq1_len > 0 else b""
    return pf1, pfq1


# ==========================
# COLD container (V1M bundle)
# ==========================

def cold_pack(bundle_blob: bytes) -> bytes:
    """
    USCC format:
      MAGIC (4)
      VERSION (u32)
      bundle_len (u32)
      bundle_blob
    """
    return MAGIC_COLD + _u32(VERSION) + _u32(len(bundle_blob)) + bundle_blob


def cold_unpack(blob: bytes) -> bytes:
    if len(blob) < 12:
        raise ValueError("bad USCC blob (too small)")
    if blob[:4] != MAGIC_COLD:
        raise ValueError("not a USCC blob")

    off = 4
    ver, off = _u32_read(blob, off)
    if ver != VERSION:
        raise ValueError(f"unsupported USCC version: {ver}")

    blen, off = _u32_read(blob, off)
    return blob[off:off + blen]


# ==========================
# Baselines
# ==========================

def baseline_gzip(raw_bytes: bytes, level: int = 9) -> bytes:
    return gzip.compress(raw_bytes, compresslevel=int(level))


def baseline_zstd(raw_bytes: bytes, level: int = 10) -> bytes:
    if zstd is None:
        raise RuntimeError("zstandard missing (pip install zstandard)")
    return zstd.ZstdCompressor(level=int(level)).compress(raw_bytes)


# ==========================
# Commands
# ==========================

def cmd_encode(args: argparse.Namespace) -> None:
    log_path = args.log
    tpl_path = args.tpl
    out_path = args.out
    lines = args.lines
    mode = args.mode.lower().strip()

    if not os.path.exists(log_path):
        raise SystemExit(f"❌ log file not found: {log_path}")
    if not os.path.exists(tpl_path):
        raise SystemExit(f"❌ template CSV not found: {tpl_path}")

    print("USC ENCODE")
    print(f"mode:   {mode}")
    print(f"log:    {log_path}")
    print(f"tpl:    {tpl_path}")
    print(f"lines:  {lines}")
    print(f"out:    {out_path}")
    print("-" * 60)

    raw_lines = _read_first_n_lines(log_path, lines)
    raw_text = "\n".join(raw_lines) + "\n"
    raw_bytes = raw_text.encode("utf-8", errors="replace")

    bank = HDFSTemplateBank.from_csv(tpl_path)
    events, unknown = parse_hdfs_lines(raw_lines, bank)

    tpl_text = open(tpl_path, "r", errors="replace").read()

    if mode == "hot":
        t0 = time.perf_counter()
        pf1_blob, _pf1_meta = build_pf1(
            events, unknown, tpl_text,
            packet_events=args.packet_events,
            zstd_level=args.zstd,
        )
        dt_pf1 = (time.perf_counter() - t0) * 1000.0

        t1 = time.perf_counter()
        pfq1_blob, _pfq1_meta = build_pfq1(
            events, unknown, tpl_text,
            packet_events=args.packet_events,
            zstd_level=args.zstd,
        )
        dt_pfq1 = (time.perf_counter() - t1) * 1000.0

        hot_blob = hot_pack(pf1_blob, pfq1_blob)
        with open(out_path, "wb") as f:
            f.write(hot_blob)

        print(f"RAW:     {_pretty(len(raw_bytes))}")
        print(f"PF1:     {_pretty(len(pf1_blob))}   build={dt_pf1:.2f} ms")
        print(f"PFQ1:    {_pretty(len(pfq1_blob))}   build={dt_pfq1:.2f} ms")
        print(f"USCH:    {_pretty(len(hot_blob))}   saved ✅")
        print(f"ratio:   {_ratio(len(raw_bytes), len(hot_blob)):.2f}x")

    elif mode == "hot-lite":
        # PF1 only (fast build, smaller hot file, no PFQ1 fallback)
        t0 = time.perf_counter()
        pf1_blob, _pf1_meta = build_pf1(
            events, unknown, tpl_text,
            packet_events=args.packet_events,
            zstd_level=args.zstd,
        )
        dt_pf1 = (time.perf_counter() - t0) * 1000.0

        hot_blob = hot_pack(pf1_blob, b"")
        with open(out_path, "wb") as f:
            f.write(hot_blob)

        print(f"RAW:     {_pretty(len(raw_bytes))}")
        print(f"PF1:     {_pretty(len(pf1_blob))}   build={dt_pf1:.2f} ms")
        print(f"USCH:    {_pretty(len(hot_blob))}   saved ✅ (HOT-LITE)")
        print(f"ratio:   {_ratio(len(raw_bytes), len(hot_blob)):.2f}x")

    elif mode == "cold":
        t0 = time.perf_counter()
        bundle_blob, _meta = bundle_encode_and_compress_v1m(
            events=events,
            unknown_lines=unknown,
            template_csv_text=tpl_text,
            zstd_level=args.zstd,
        )
        dt = (time.perf_counter() - t0) * 1000.0

        cold_blob = cold_pack(bundle_blob)
        with open(out_path, "wb") as f:
            f.write(cold_blob)

        print(f"RAW:     {_pretty(len(raw_bytes))}")
        print(f"BUNDLE:  {_pretty(len(bundle_blob))}   build={dt:.2f} ms")
        print(f"USCC:    {_pretty(len(cold_blob))}   saved ✅")
        print(f"ratio:   {_ratio(len(raw_bytes), len(cold_blob)):.2f}x")

    else:
        raise SystemExit("❌ mode must be: hot | hot-lite | cold")

    print("-" * 60)
    print("DONE ✅")


def cmd_query(args: argparse.Namespace) -> None:
    hot_path = args.hot
    q = args.q
    limit = args.limit

    if not os.path.exists(hot_path):
        raise SystemExit(f"❌ hot file not found: {hot_path}")

    blob = open(hot_path, "rb").read()
    pf1_blob, pfq1_blob = hot_unpack(blob)

    print("USC QUERY (HOT)")
    print(f"hot:   {hot_path}")
    print(f"q:     {q!r}")
    print(f"limit: {limit}")
    print("-" * 60)

    t0 = time.perf_counter()

    # HOT-LITE: PFQ1 missing → FAST-only
    if len(pfq1_blob) == 0:
        hits, cands = query_fast_pf1(pf1_blob, q, limit=limit)
        dt = (time.perf_counter() - t0) * 1000.0

        print("mode: FAST (HOT-LITE)")
        print(f"hits: {len(hits)}   time={dt:.2f} ms")
        if not hits:
            print("-" * 60)
            print("No hits found in FAST-only mode.")
            print("Tip: encode with --mode hot (includes PFQ1 fallback) for universal keyword search.")
        elif hits:
            print("-" * 60)
            for i, h in enumerate(hits[: min(limit, 10)]):
                print(f"{i+1:02d}) {h[:200]}")
        print("-" * 60)
        print("DONE ✅")
        return

    # Full HOT: router FAST → PFQ1 fallback
    hits, mode = query_router_v1(pf1_blob, pfq1_blob, q, limit=limit)
    dt = (time.perf_counter() - t0) * 1000.0

    print(f"mode: {mode}")
    print(f"hits: {len(hits)}   time={dt:.2f} ms")
    if hits:
        print("-" * 60)
        for i, h in enumerate(hits[: min(limit, 10)]):
            print(f"{i+1:02d}) {h[:200]}")
    print("-" * 60)
    print("DONE ✅")


def _row(name: str, size: int, raw: int, ms: float) -> Dict[str, object]:
    return {
        "name": name,
        "bytes": int(size),
        "pretty": _pretty(int(size)),
        "ratio": float(_ratio(raw, size)),
        "ms": float(ms),
    }


def cmd_bench(args: argparse.Namespace) -> None:
    log_path = args.log
    tpl_path = args.tpl
    lines = args.lines

    if not os.path.exists(log_path):
        raise SystemExit(f"❌ log file not found: {log_path}")
    if not os.path.exists(tpl_path):
        raise SystemExit(f"❌ template CSV not found: {tpl_path}")

    raw_lines = _read_first_n_lines(log_path, lines)
    raw_text = "\n".join(raw_lines) + "\n"
    raw_bytes = raw_text.encode("utf-8", errors="replace")
    raw_n = len(raw_bytes)

    bank = HDFSTemplateBank.from_csv(tpl_path)
    events, unknown = parse_hdfs_lines(raw_lines, bank)
    tpl_text = open(tpl_path, "r", errors="replace").read()

    print("USC BENCH — Scoreboard")
    print(f"log:   {log_path}")
    print(f"tpl:   {tpl_path}")
    print(f"lines: {len(raw_lines)}")
    print(f"RAW:   {_pretty(raw_n)}")
    print("-" * 72)

    rows: List[Dict[str, object]] = []

    # --- gzip baseline
    t0 = time.perf_counter()
    gz = baseline_gzip(raw_bytes, level=args.gzip)
    t_gz = (time.perf_counter() - t0) * 1000.0
    rows.append(_row(f"gzip-{args.gzip}", len(gz), raw_n, t_gz))

    # --- zstd baseline
    if zstd is not None:
        t0 = time.perf_counter()
        zs = baseline_zstd(raw_bytes, level=args.zstd)
        t_zs = (time.perf_counter() - t0) * 1000.0
        rows.append(_row(f"zstd-{args.zstd}", len(zs), raw_n, t_zs))
    else:
        rows.append({"name": f"zstd-{args.zstd}", "bytes": None, "pretty": "N/A", "ratio": None, "ms": None})

    # --- USC HOT-LITE (PF1 only)
    t0 = time.perf_counter()
    pf1_blob, _m1 = build_pf1(events, unknown, tpl_text, packet_events=args.packet_events, zstd_level=args.zstd)
    t_pf1 = (time.perf_counter() - t0) * 1000.0
    usch_lite = hot_pack(pf1_blob, b"")
    rows.append(_row("USC-HOT-LITE (PF1)", len(usch_lite), raw_n, t_pf1))

    # --- USC HOT (PF1 + PFQ1)
    t0 = time.perf_counter()
    pf1_blob2, _m2 = build_pf1(events, unknown, tpl_text, packet_events=args.packet_events, zstd_level=args.zstd)
    t_pf1b = (time.perf_counter() - t0) * 1000.0

    t0 = time.perf_counter()
    pfq1_blob, _m3 = build_pfq1(events, unknown, tpl_text, packet_events=args.packet_events, zstd_level=args.zstd)
    t_pfq1 = (time.perf_counter() - t0) * 1000.0

    usch = hot_pack(pf1_blob2, pfq1_blob)
    rows.append(_row("USC-HOT (USCH)", len(usch), raw_n, t_pf1b + t_pfq1))

    # --- USC COLD (USCC)
    t0 = time.perf_counter()
    bundle_blob, _meta = bundle_encode_and_compress_v1m(
        events=events,
        unknown_lines=unknown,
        template_csv_text=tpl_text,
        zstd_level=args.zstd,
    )
    t_cold = (time.perf_counter() - t0) * 1000.0

    uscc = cold_pack(bundle_blob)
    rows.append(_row("USC-COLD (USCC)", len(uscc), raw_n, t_cold))

    rows_sorted = sorted([r for r in rows if r.get("bytes") is not None], key=lambda x: x["bytes"])

    print(f"{'METHOD':24} {'SIZE':12} {'RATIO':10} {'BUILD(ms)':10}")
    print("-" * 72)
    for r in rows_sorted:
        print(f"{r['name'][:24]:24} {r['pretty']:12} {r['ratio']:>9.2f}x {r['ms']:>10.2f}")

    print("-" * 72)
    print("NOTES:")
    print(" - HOT-LITE stores PF1 only (FAST queries, no PFQ1 fallback).")
    print(" - HOT stores PF1 + PFQ1 (FAST + universal fallback).")
    print(" - COLD stores max-compression bundle archive.")
    print(" - gzip/zstd are general compressors (not queryable).")
    print("-" * 72)

    if args.out_json:
        payload = {
            "ts": time.time(),
            "log": log_path,
            "tpl": tpl_path,
            "lines": len(raw_lines),
            "raw_bytes": raw_n,
            "packet_events": args.packet_events,
            "gzip_level": args.gzip,
            "zstd_level": args.zstd,
            "results": rows_sorted,
        }
        with open(args.out_json, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
        print(f"saved_json: {args.out_json} ✅")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="usc",
        description="Unified State Codec (USC) — Hot/Cool compression + query",
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    enc = sub.add_parser("encode", help="Encode a log into HOT/HOT-LITE (queryable) or COLD (max ratio)")
    enc.add_argument("--mode", choices=["hot", "hot-lite", "cold"], required=True)
    enc.add_argument("--log", default="data/loghub/HDFS.log")
    enc.add_argument("--tpl", default="data/loghub/preprocessed/HDFS.log_templates.csv")
    enc.add_argument("--out", required=True)
    enc.add_argument("--lines", type=int, default=200000)
    enc.add_argument("--packet_events", type=int, default=32768)
    enc.add_argument("--zstd", type=int, default=10)
    enc.set_defaults(func=cmd_encode)

    qry = sub.add_parser("query", help="Query a HOT/HOT-LITE USC blob")
    qry.add_argument("--hot", required=True)
    qry.add_argument("--q", required=True)
    qry.add_argument("--limit", type=int, default=25)
    qry.set_defaults(func=cmd_query)

    b = sub.add_parser("bench", help="Run baselines + USC modes and print a scoreboard")
    b.add_argument("--log", default="data/loghub/HDFS.log")
    b.add_argument("--tpl", default="data/loghub/preprocessed/HDFS.log_templates.csv")
    b.add_argument("--lines", type=int, default=200000)
    b.add_argument("--packet_events", type=int, default=32768)
    b.add_argument("--gzip", type=int, default=9)
    b.add_argument("--zstd", type=int, default=10)
    b.add_argument("--out_json", default=None)
    b.set_defaults(func=cmd_bench)

    return p


def main():
    p = build_parser()
    args = p.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
