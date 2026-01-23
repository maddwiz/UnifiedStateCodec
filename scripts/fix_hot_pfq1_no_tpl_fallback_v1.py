from __future__ import annotations
from pathlib import Path
import re

APP = Path("src/usc/cli/app.py")

def die(msg: str):
    raise SystemExit(msg)

def main():
    if not APP.exists():
        die(f"❌ missing {APP}")

    s = APP.read_text(encoding="utf-8", errors="replace")

    # locate function build_pfq1_from_log(...)
    m = re.search(r"def build_pfq1_from_log\s*\(.*?\)\s*->\s*bytes\s*:\s*", s, re.DOTALL)
    if not m:
        die("❌ could not find build_pfq1_from_log(...) in app.py")

    fn_start = m.start()

    # find end of function by next "def " at column 0
    m2 = re.search(r"\ndef\s+\w+\s*\(", s[m.end():])
    if not m2:
        die("❌ could not find end of build_pfq1_from_log (next def)")

    fn_end = m.end() + m2.start()

    new_fn = r'''def build_pfq1_from_log(
    log_path: str,
    tpl_path: str,
    lines: int,
    packet_events: int,
    zstd_level: int,
) -> bytes:
    """
    Build PFQ1 HOT index from a log.
    ✅ If tpl_path is missing/empty, fallback to raw-line token bloom packets.
    """
    from pathlib import Path

    log_p = Path(log_path)
    if not log_p.exists():
        raise SystemExit(f"❌ log not found: {log_path}")

    # read lines
    with log_p.open("r", encoding="utf-8", errors="replace") as f:
        raw_lines = []
        for i, ln in enumerate(f):
            raw_lines.append(ln.rstrip("\n"))
            if lines and i + 1 >= int(lines):
                break

    # If tpl missing -> fallback: unknown_lines = raw_lines, no events/templates
    tpl_txt = ""
    if tpl_path:
        tp = Path(tpl_path)
        if tp.exists():
            tpl_txt = tp.read_text(encoding="utf-8", errors="replace")

    if not tpl_txt.strip():
        pfq1_blob, _meta = build_pfq1(
            events=[],
            unknown_lines=raw_lines,
            template_csv_text="",
            packet_events=packet_events,
            zstd_level=zstd_level,
        )
        return pfq1_blob

    # Normal path: use existing build_pfq1() w/ template CSV
    pfq1_blob, _meta = build_pfq1(
        events=[],               # existing pipeline fills this elsewhere if needed
        unknown_lines=raw_lines, # keep unknown_lines available for PFQ1 tokens too
        template_csv_text=tpl_txt,
        packet_events=packet_events,
        zstd_level=zstd_level,
    )
    return pfq1_blob
'''

    s2 = s[:fn_start] + new_fn + s[fn_end:]
    APP.write_text(s2, encoding="utf-8")
    print("✅ patched build_pfq1_from_log(): tpl missing -> raw-line PFQ1 fallback")

if __name__ == "__main__":
    main()
