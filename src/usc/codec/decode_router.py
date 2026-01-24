from __future__ import annotations
from pathlib import Path

def sniff_magic(path: Path) -> bytes:
    with path.open("rb") as f:
        return f.read(4)

def decode_auto(in_path: str, out_path: str) -> str:
    """
    Auto-detect container format and route to correct decoder.
    Returns the detected mode label.
    """
    p = Path(in_path)
    magic = sniff_magic(p)
    blob = p.read_bytes()

    # PF3 (HOT-LITE-FULL)  -> magic = TPF3
    if magic == b"TPF3":
        from usc.mem.tpl_pf3_decode_v1_h1m2 import decode_pf3_h1m2_to_lines
        lines = decode_pf3_h1m2_to_lines(blob)
        Path(out_path).write_text("\n".join(lines) + "\n", encoding="utf-8")
        return "hot-lite-full"

    # HOT (USCH + PFQ1) -> magic = USCH
    if magic == b"USCH":
        raise SystemExit("HOT decode not wired yet (USCH). Next step will add it.")

    # COLD (USCC bundle) -> magic = USCC
    if magic == b"USCC":
        raise SystemExit("COLD decode not wired yet (USCC). Next step will add it.")

    raise SystemExit(f"Unknown file magic: {magic!r} (expected TPF3 / USCH / USCC)")
